package main

import (
	"context"
	crand "crypto/rand"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	gsm "github.com/bradleypeabody/gorilla-sessions-memcache"
	"github.com/go-chi/chi/v5"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/gorilla/sessions"
	"github.com/jmoiron/sqlx"
)

var (
	db    *sqlx.DB
	store *gsm.MemcacheStore

	// users はBAN・登録以外ほぼ不変なので、id 引きをアプリ内にキャッシュする。
	// DBが正で、変更時(BAN/初期化)に無効化することで整合性を保つ。
	userCacheMu sync.RWMutex
	userCache   = map[int]User{}
)

func getUserByID(ctx context.Context, id int) (User, error) {
	userCacheMu.RLock()
	u, ok := userCache[id]
	userCacheMu.RUnlock()
	if ok {
		return u, nil
	}

	var nu User
	if err := db.GetContext(ctx, &nu, "SELECT * FROM `users` WHERE `id` = ?", id); err != nil {
		return nu, err
	}
	userCacheMu.Lock()
	userCache[id] = nu
	userCacheMu.Unlock()
	return nu, nil
}

func invalidateUserCache(id int) {
	userCacheMu.Lock()
	delete(userCache, id)
	userCacheMu.Unlock()
}

func clearUserCache() {
	userCacheMu.Lock()
	userCache = map[int]User{}
	userCacheMu.Unlock()
}

const (
	postsPerPage  = 20
	ISO8601Format = "2006-01-02T15:04:05-07:00"
	UploadLimit   = 10 * 1024 * 1024 // 10mb
	imageDir      = "../public/image"
)

// writeImageFile は画像をファイルに書き出す。以降の同一画像へのリクエストは
// nginx が public/image/ から直接配信し、アプリと DB の負荷を肩代わりする。
// 部分的に書きかけのファイルを nginx が配信しないよう、一時ファイルへ書いてから rename する。
func writeImageFile(id int, ext string, data []byte) error {
	if ext == "" {
		return nil
	}
	f, err := os.CreateTemp(imageDir, "tmp-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Chmod(0644)
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, fmt.Sprintf("%s/%d.%s", imageDir, id, ext))
}

type User struct {
	ID          int       `db:"id"`
	AccountName string    `db:"account_name"`
	Passhash    string    `db:"passhash"`
	Authority   int       `db:"authority"`
	DelFlg      int       `db:"del_flg"`
	CreatedAt   time.Time `db:"created_at"`
}

type Post struct {
	ID           int       `db:"id"`
	UserID       int       `db:"user_id"`
	Imgdata      []byte    `db:"imgdata"`
	Body         string    `db:"body"`
	Mime         string    `db:"mime"`
	CreatedAt    time.Time `db:"created_at"`
	CommentCount int
	Comments     []Comment
	User         User
	CSRFToken    string
	// テンプレートでの reflect 経由の関数/メソッド呼び出し(imageURL, CreatedAt.Format)を
	// 避けるため Go 側で事前計算しておく。出力は従来と同一。
	ImageURL     string
	CreatedAtFmt string
}

type Comment struct {
	ID        int       `db:"id"`
	PostID    int       `db:"post_id"`
	UserID    int       `db:"user_id"`
	Comment   string    `db:"comment"`
	CreatedAt time.Time `db:"created_at"`
	User      User
}

var memcacheClient *memcache.Client

func init() {
	memdAddr := os.Getenv("ISUCONP_MEMCACHED_ADDRESS")
	if memdAddr == "" {
		memdAddr = "localhost:11211"
	}
	memcacheClient = memcache.New(memdAddr)
	store = gsm.NewMemcacheStore(memcacheClient, "iscogram_", []byte("sendagaya"))
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func dbInitialize(ctx context.Context) {
	sqls := []string{
		"DELETE FROM users WHERE id > 1000",
		"DELETE FROM posts WHERE id > 10000",
		"DELETE FROM comments WHERE id > 100000",
		"UPDATE users SET del_flg = 0",
		"UPDATE users SET del_flg = 1 WHERE id % 50 = 0",
	}

	for _, sql := range sqls {
		db.ExecContext(ctx, sql)
	}

	// 初期化で users の削除・del_flg 更新が走るため、キャッシュを破棄する
	clearUserCache()

	// ベンチのアップロードで作られた id>10000 の画像ファイルを掃除しつつ、
	// ディスク上に存在する画像 id を集める。
	present := map[int]bool{}
	if entries, err := os.ReadDir(imageDir); err == nil {
		for _, e := range entries {
			name := e.Name()
			dot := strings.IndexByte(name, '.')
			if dot <= 0 {
				continue
			}
			id, err := strconv.Atoi(name[:dot])
			if err != nil {
				continue
			}
			if id > 10000 {
				os.Remove(imageDir + "/" + name)
			} else {
				present[id] = true
			}
		}
	}

	// 初期データ(id<=10000)の画像をすべてファイル化しておく。これにより全画像を
	// nginx が直接配信でき、getImage 経由の imgdata(BLOB) 読み込みが発生しなくなる
	// （= 巨大 BLOB がバッファプールを汚さない）。ファイルが既にあるものは skip するので
	// 通常は DB アクセスゼロ。dump 直後など未生成の場合だけ materialize する。
	var metas []Post
	if err := db.SelectContext(ctx, &metas, "SELECT `id`, `mime` FROM `posts` WHERE `id` <= 10000"); err == nil {
		for _, m := range metas {
			if present[m.ID] {
				continue
			}
			ext := ""
			switch m.Mime {
			case "image/jpeg":
				ext = "jpg"
			case "image/png":
				ext = "png"
			case "image/gif":
				ext = "gif"
			default:
				continue
			}
			var img []byte
			if err := db.GetContext(ctx, &img, "SELECT `imgdata` FROM `posts` WHERE `id` = ?", m.ID); err != nil {
				continue
			}
			if err := writeImageFile(m.ID, ext, img); err != nil {
				log.Print(err)
			}
		}
	}
}

func tryLogin(ctx context.Context, accountName, password string) *User {
	u := User{}
	err := db.GetContext(ctx, &u, "SELECT * FROM users WHERE account_name = ? AND del_flg = 0", accountName)
	if err != nil {
		return nil
	}

	if calculatePasshash(ctx, u.AccountName, password) == u.Passhash {
		return &u
	} else {
		return nil
	}
}

func validateUser(accountName, password string) bool {
	return regexp.MustCompile(`\A[0-9a-zA-Z_]{3,}\z`).MatchString(accountName) &&
		regexp.MustCompile(`\A[0-9a-zA-Z_]{6,}\z`).MatchString(password)
}

// 旧実装は openssl dgst -sha512 を外部プロセスで実行していたが、
// SHA-512 は Go 標準ライブラリで計算できる。出力は openssl と同じ小文字hex。
func digest(ctx context.Context, src string) string {
	sum := sha512.Sum512([]byte(src))
	return hex.EncodeToString(sum[:])
}

func calculateSalt(ctx context.Context, accountName string) string {
	return digest(ctx, accountName)
}

func calculatePasshash(ctx context.Context, accountName, password string) string {
	return digest(ctx, password+":"+calculateSalt(ctx, accountName))
}

func getSession(r *http.Request) *sessions.Session {
	session, _ := store.Get(r, "isuconp-go.session")

	return session
}

func getSessionUser(r *http.Request) User {
	ctx := r.Context()
	session := getSession(r)
	uid, ok := session.Values["user_id"]
	if !ok || uid == nil {
		return User{}
	}

	id, ok := uid.(int)
	if !ok {
		switch v := uid.(type) {
		case int64:
			id = int(v)
		default:
			return User{}
		}
	}

	u, err := getUserByID(ctx, id)
	if err != nil {
		return User{}
	}

	return u
}

func getFlash(w http.ResponseWriter, r *http.Request, key string) string {
	session := getSession(r)
	value, ok := session.Values[key]

	if !ok || value == nil {
		return ""
	} else {
		delete(session.Values, key)
		session.Save(r, w)
		return value.(string)
	}
}

func makePosts(ctx context.Context, results []Post, csrfToken string, allComments bool) ([]Post, error) {
	// 1) del_flg=0 のユーザーの投稿だけを postsPerPage 件まで採用する（元の挙動を踏襲）
	var selected []Post
	for _, p := range results {
		u, err := getUserByID(ctx, p.UserID)
		if err != nil {
			return nil, err
		}
		if u.DelFlg != 0 {
			continue
		}
		p.User = u
		p.CSRFToken = csrfToken
		selected = append(selected, p)
		if len(selected) >= postsPerPage {
			break
		}
	}
	if len(selected) == 0 {
		return []Post{}, nil
	}

	postIDs := make([]int, len(selected))
	for i := range selected {
		postIDs[i] = selected[i].ID
	}

	// 2) コメント数を1クエリで一括集計（投稿ごとの COUNT N+1 を排除）
	countMap := map[int]int{}
	{
		type commentCount struct {
			PostID int `db:"post_id"`
			Count  int `db:"count"`
		}
		q, args, err := sqlx.In("SELECT `post_id`, COUNT(*) AS `count` FROM `comments` WHERE `post_id` IN (?) GROUP BY `post_id`", postIDs)
		if err != nil {
			return nil, err
		}
		var counts []commentCount
		if err := db.SelectContext(ctx, &counts, q, args...); err != nil {
			return nil, err
		}
		for _, c := range counts {
			countMap[c.PostID] = c.Count
		}
	}

	// 3) コメント本体を1クエリで一括取得（一覧は各投稿の新着3件、詳細は全件）
	commentsByPost := map[int][]Comment{}
	{
		var q string
		var args []interface{}
		var err error
		if allComments {
			q, args, err = sqlx.In("SELECT `id`, `post_id`, `user_id`, `comment`, `created_at` FROM `comments` WHERE `post_id` IN (?) ORDER BY `created_at` DESC", postIDs)
		} else {
			// 各 post_id ごとの新着3件をウィンドウ関数で取得
			q, args, err = sqlx.In("SELECT `id`, `post_id`, `user_id`, `comment`, `created_at` FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY `post_id` ORDER BY `created_at` DESC) AS `rn` FROM `comments` WHERE `post_id` IN (?)) `t` WHERE `rn` <= 3 ORDER BY `created_at` DESC", postIDs)
		}
		if err != nil {
			return nil, err
		}
		var comments []Comment
		if err := db.SelectContext(ctx, &comments, q, args...); err != nil {
			return nil, err
		}
		for i := range comments {
			comments[i].User, err = getUserByID(ctx, comments[i].UserID)
			if err != nil {
				return nil, err
			}
			commentsByPost[comments[i].PostID] = append(commentsByPost[comments[i].PostID], comments[i])
		}
	}

	// 4) 組み立て（コメントは created_at DESC で集めたので表示用に昇順へ反転）
	posts := make([]Post, 0, len(selected))
	for _, p := range selected {
		p.CommentCount = countMap[p.ID]
		comments := commentsByPost[p.ID]
		for i, j := 0, len(comments)-1; i < j; i, j = i+1, j-1 {
			comments[i], comments[j] = comments[j], comments[i]
		}
		p.Comments = comments
		// テンプレート描画時の reflect 呼び出しを避けるため事前計算
		p.ImageURL = imageURL(p)
		p.CreatedAtFmt = p.CreatedAt.Format("2006-01-02T15:04:05-07:00")
		posts = append(posts, p)
	}

	return posts, nil
}

func imageURL(p Post) string {
	ext := ""
	if p.Mime == "image/jpeg" {
		ext = ".jpg"
	} else if p.Mime == "image/png" {
		ext = ".png"
	} else if p.Mime == "image/gif" {
		ext = ".gif"
	}

	return "/image/" + strconv.Itoa(p.ID) + ext
}

func isLogin(u User) bool {
	return u.ID != 0
}

func getCSRFToken(r *http.Request) string {
	session := getSession(r)
	csrfToken, ok := session.Values["csrf_token"]
	if !ok {
		return ""
	}
	return csrfToken.(string)
}

func secureRandomStr(b int) string {
	k := make([]byte, b)
	if _, err := crand.Read(k); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", k)
}

func getTemplPath(filename string) string {
	return path.Join("templates", filename)
}

// テンプレートは起動時に1回だけ parse する。以前は各ハンドラ内で毎リクエスト
// ParseFiles していたため、CPU を大きく消費していた（GET / は4ファイル/リクエスト）。
// Execute が描画する root テンプレート名（先頭ファイル）は従来と同一に保つ。
var (
	tmplFuncMap = template.FuncMap{"imageURL": imageURL}

	tmplLogin       = template.Must(template.ParseFiles(getTemplPath("layout.html"), getTemplPath("login.html")))
	tmplRegister    = template.Must(template.ParseFiles(getTemplPath("layout.html"), getTemplPath("register.html")))
	tmplAdminBanned = template.Must(template.ParseFiles(getTemplPath("layout.html"), getTemplPath("banned.html")))

	tmplIndex = template.Must(template.New("layout.html").Funcs(tmplFuncMap).ParseFiles(
		getTemplPath("layout.html"), getTemplPath("index.html"), getTemplPath("posts.html"), getTemplPath("post.html")))
	tmplAccount = template.Must(template.New("layout.html").Funcs(tmplFuncMap).ParseFiles(
		getTemplPath("layout.html"), getTemplPath("user.html"), getTemplPath("posts.html"), getTemplPath("post.html")))
	tmplPosts = template.Must(template.New("posts.html").Funcs(tmplFuncMap).ParseFiles(
		getTemplPath("posts.html"), getTemplPath("post.html")))
	tmplPostID = template.Must(template.New("layout.html").Funcs(tmplFuncMap).ParseFiles(
		getTemplPath("layout.html"), getTemplPath("post_id.html"), getTemplPath("post.html")))
)

func getInitialize(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbInitialize(ctx)
	w.WriteHeader(http.StatusOK)
}

func getLogin(w http.ResponseWriter, r *http.Request) {
	me := getSessionUser(r)

	if isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	tmplLogin.Execute(w, struct {
		Me    User
		Flash string
	}{me, getFlash(w, r, "notice")})
}

func postLogin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isLogin(getSessionUser(r)) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	u := tryLogin(ctx, r.FormValue("account_name"), r.FormValue("password"))

	if u != nil {
		session := getSession(r)
		session.Values["user_id"] = u.ID
		session.Values["csrf_token"] = secureRandomStr(16)
		session.Save(r, w)

		http.Redirect(w, r, "/", http.StatusFound)
	} else {
		session := getSession(r)
		session.Values["notice"] = "アカウント名かパスワードが間違っています"
		session.Save(r, w)

		http.Redirect(w, r, "/login", http.StatusFound)
	}
}

func getRegister(w http.ResponseWriter, r *http.Request) {
	if isLogin(getSessionUser(r)) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	tmplRegister.Execute(w, struct {
		Me    User
		Flash string
	}{User{}, getFlash(w, r, "notice")})
}

func postRegister(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isLogin(getSessionUser(r)) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	accountName, password := r.FormValue("account_name"), r.FormValue("password")

	validated := validateUser(accountName, password)
	if !validated {
		session := getSession(r)
		session.Values["notice"] = "アカウント名は3文字以上、パスワードは6文字以上である必要があります"
		session.Save(r, w)

		http.Redirect(w, r, "/register", http.StatusFound)
		return
	}

	exists := 0
	// ユーザーが存在しない場合はエラーになるのでエラーチェックはしない
	db.GetContext(ctx, &exists, "SELECT 1 FROM users WHERE `account_name` = ?", accountName)

	if exists == 1 {
		session := getSession(r)
		session.Values["notice"] = "アカウント名がすでに使われています"
		session.Save(r, w)

		http.Redirect(w, r, "/register", http.StatusFound)
		return
	}

	query := "INSERT INTO `users` (`account_name`, `passhash`) VALUES (?,?)"
	result, err := db.ExecContext(ctx, query, accountName, calculatePasshash(ctx, accountName, password))
	if err != nil {
		log.Print(err)
		return
	}

	session := getSession(r)
	uid, err := result.LastInsertId()
	if err != nil {
		log.Print(err)
		return
	}
	session.Values["user_id"] = uid
	session.Values["csrf_token"] = secureRandomStr(16)
	session.Save(r, w)

	http.Redirect(w, r, "/", http.StatusFound)
}

func getLogout(w http.ResponseWriter, r *http.Request) {
	session := getSession(r)
	delete(session.Values, "user_id")
	session.Options = &sessions.Options{MaxAge: -1}
	session.Save(r, w)

	http.Redirect(w, r, "/", http.StatusFound)
}

func getIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me := getSessionUser(r)

	results := []Post{}

	// del_flg=0 のユーザーの投稿だけを新着順に postsPerPage 件取得する。
	// users と JOIN して SQL 側で絞り込み、posts の全件スキャンを避ける。
	// posts(user_id) 系の索引が増えるとオプティマイザが users 全走査の悪いプランを
	// 選ぶため、新着順の索引を FORCE INDEX で固定する。
	err := db.SelectContext(ctx, &results, "SELECT p.`id`, p.`user_id`, p.`body`, p.`mime`, p.`created_at` FROM `posts` p FORCE INDEX (idx_created_at) JOIN `users` u ON p.`user_id` = u.`id` WHERE u.`del_flg` = 0 ORDER BY p.`created_at` DESC LIMIT ?", postsPerPage)
	if err != nil {
		log.Print(err)
		return
	}

	posts, err := makePosts(ctx, results, getCSRFToken(r), false)
	if err != nil {
		log.Print(err)
		return
	}

	tmplIndex.Execute(w, struct {
		Posts     []Post
		Me        User
		CSRFToken string
		Flash     string
	}{posts, me, getCSRFToken(r), getFlash(w, r, "notice")})
}

func getAccountName(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	accountName := r.PathValue("accountName")
	user := User{}

	err := db.GetContext(ctx, &user, "SELECT * FROM `users` WHERE `account_name` = ? AND `del_flg` = 0", accountName)
	if err != nil {
		log.Print(err)
		return
	}

	if user.ID == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	results := []Post{}

	err = db.SelectContext(ctx, &results, "SELECT `id`, `user_id`, `body`, `mime`, `created_at` FROM `posts` WHERE `user_id` = ? ORDER BY `created_at` DESC", user.ID)
	if err != nil {
		log.Print(err)
		return
	}

	posts, err := makePosts(ctx, results, getCSRFToken(r), false)
	if err != nil {
		log.Print(err)
		return
	}

	commentCount := 0
	err = db.GetContext(ctx, &commentCount, "SELECT COUNT(*) AS count FROM `comments` WHERE `user_id` = ?", user.ID)
	if err != nil {
		log.Print(err)
		return
	}

	postIDs := []int{}
	err = db.SelectContext(ctx, &postIDs, "SELECT `id` FROM `posts` WHERE `user_id` = ?", user.ID)
	if err != nil {
		log.Print(err)
		return
	}
	postCount := len(postIDs)

	commentedCount := 0
	if postCount > 0 {
		s := []string{}
		for range postIDs {
			s = append(s, "?")
		}
		placeholder := strings.Join(s, ", ")

		// convert []int -> []any
		args := make([]any, len(postIDs))
		for i, v := range postIDs {
			args[i] = v
		}

		err = db.GetContext(ctx, &commentedCount, "SELECT COUNT(*) AS count FROM `comments` WHERE `post_id` IN ("+placeholder+")", args...)
		if err != nil {
			log.Print(err)
			return
		}
	}

	me := getSessionUser(r)

	tmplAccount.Execute(w, struct {
		Posts          []Post
		User           User
		PostCount      int
		CommentCount   int
		CommentedCount int
		Me             User
	}{posts, user, postCount, commentCount, commentedCount, me})
}

func getPosts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	m, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Print(err)
		return
	}
	maxCreatedAt := m.Get("max_created_at")
	if maxCreatedAt == "" {
		return
	}

	t, err := time.Parse(ISO8601Format, maxCreatedAt)
	if err != nil {
		log.Print(err)
		return
	}

	results := []Post{}
	err = db.SelectContext(ctx, &results, "SELECT p.`id`, p.`user_id`, p.`body`, p.`mime`, p.`created_at` FROM `posts` p FORCE INDEX (idx_created_at) JOIN `users` u ON p.`user_id` = u.`id` WHERE u.`del_flg` = 0 AND p.`created_at` <= ? ORDER BY p.`created_at` DESC LIMIT ?", t.Format(ISO8601Format), postsPerPage)
	if err != nil {
		log.Print(err)
		return
	}

	posts, err := makePosts(ctx, results, getCSRFToken(r), false)
	if err != nil {
		log.Print(err)
		return
	}

	if len(posts) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	tmplPosts.Execute(w, posts)
}

func getPostsID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pidStr := r.PathValue("id")
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	results := []Post{}
	// imgdata(BLOB)は描画に不要なので取得しない。SELECT * だと巨大なBLOBを毎回
	// バッファプールへ載せ、ホットなメタデータページを追い出していた。
	err = db.SelectContext(ctx, &results, "SELECT `id`, `user_id`, `body`, `mime`, `created_at` FROM `posts` WHERE `id` = ?", pid)
	if err != nil {
		log.Print(err)
		return
	}

	posts, err := makePosts(ctx, results, getCSRFToken(r), true)
	if err != nil {
		log.Print(err)
		return
	}

	if len(posts) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	p := posts[0]

	me := getSessionUser(r)

	tmplPostID.Execute(w, struct {
		Post Post
		Me   User
	}{p, me})
}

func postIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	if r.FormValue("csrf_token") != getCSRFToken(r) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		session := getSession(r)
		session.Values["notice"] = "画像が必須です"
		session.Save(r, w)

		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	mime := ""
	if file != nil {
		// 投稿のContent-Typeからファイルのタイプを決定する
		contentType := header.Header["Content-Type"][0]
		if strings.Contains(contentType, "jpeg") {
			mime = "image/jpeg"
		} else if strings.Contains(contentType, "png") {
			mime = "image/png"
		} else if strings.Contains(contentType, "gif") {
			mime = "image/gif"
		} else {
			session := getSession(r)
			session.Values["notice"] = "投稿できる画像形式はjpgとpngとgifだけです"
			session.Save(r, w)

			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
	}

	filedata, err := io.ReadAll(file)
	if err != nil {
		log.Print(err)
		return
	}

	if len(filedata) > UploadLimit {
		session := getSession(r)
		session.Values["notice"] = "ファイルサイズが大きすぎます"
		session.Save(r, w)

		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	// 画像はファイル(public/image/)に書き出して nginx が直接配信する。DBには
	// imgdata BLOB を保存しない(空)。posts テーブルを小さく保ち、バッファプールに
	// 完全常駐させるため。INSERT の書込量も大幅に減る。
	query := "INSERT INTO `posts` (`user_id`, `mime`, `imgdata`, `body`) VALUES (?,?,?,?)"
	result, err := db.ExecContext(
		ctx,
		query,
		me.ID,
		mime,
		[]byte{},
		r.FormValue("body"),
	)
	if err != nil {
		log.Print(err)
		return
	}

	pid, err := result.LastInsertId()
	if err != nil {
		log.Print(err)
		return
	}

	// POST 直後の GET で参照できるよう、アップロード画像をファイルにも書き出す
	ext := ""
	switch mime {
	case "image/jpeg":
		ext = "jpg"
	case "image/png":
		ext = "png"
	case "image/gif":
		ext = "gif"
	}
	if err := writeImageFile(int(pid), ext, filedata); err != nil {
		log.Print(err)
	}

	http.Redirect(w, r, "/posts/"+strconv.FormatInt(pid, 10), http.StatusFound)
}

func getImage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pidStr := r.PathValue("id")
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 必要なのは mime と imgdata だけ。通常はファイルが存在し nginx が直接配信する
	// ため、ここに来るのはファイル未生成の初回のみ。
	post := Post{}
	err = db.GetContext(ctx, &post, "SELECT `mime`, `imgdata` FROM `posts` WHERE `id` = ?", pid)
	if err != nil {
		log.Print(err)
		return
	}

	ext := r.PathValue("ext")

	if ext == "jpg" && post.Mime == "image/jpeg" ||
		ext == "png" && post.Mime == "image/png" ||
		ext == "gif" && post.Mime == "image/gif" {
		// 初回アクセス時にファイルへ書き出し、以降は nginx が直接配信する
		if err := writeImageFile(pid, ext, post.Imgdata); err != nil {
			log.Print(err)
		}
		w.Header().Set("Content-Type", post.Mime)
		_, err := w.Write(post.Imgdata)
		if err != nil {
			log.Print(err)
			return
		}
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func postComment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	if r.FormValue("csrf_token") != getCSRFToken(r) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	postID, err := strconv.Atoi(r.FormValue("post_id"))
	if err != nil {
		log.Print("post_idは整数のみです")
		return
	}

	query := "INSERT INTO `comments` (`post_id`, `user_id`, `comment`) VALUES (?,?,?)"
	_, err = db.ExecContext(ctx, query, postID, me.ID, r.FormValue("comment"))
	if err != nil {
		log.Print(err)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/posts/%d", postID), http.StatusFound)
}

func getAdminBanned(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	if me.Authority == 0 {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	users := []User{}
	err := db.SelectContext(ctx, &users, "SELECT * FROM `users` WHERE `authority` = 0 AND `del_flg` = 0 ORDER BY `created_at` DESC")
	if err != nil {
		log.Print(err)
		return
	}

	tmplAdminBanned.Execute(w, struct {
		Users     []User
		Me        User
		CSRFToken string
	}{users, me, getCSRFToken(r)})
}

func postAdminBanned(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	if me.Authority == 0 {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if r.FormValue("csrf_token") != getCSRFToken(r) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	query := "UPDATE `users` SET `del_flg` = ? WHERE `id` = ?"

	err := r.ParseForm()
	if err != nil {
		log.Print(err)
		return
	}

	for _, id := range r.Form["uid[]"] {
		db.ExecContext(ctx, query, 1, id)
		// BAN したユーザーは del_flg が変わるのでキャッシュを無効化する
		if n, err := strconv.Atoi(id); err == nil {
			invalidateUserCache(n)
		}
	}

	http.Redirect(w, r, "/admin/banned", http.StatusFound)
}

func main() {
	host := os.Getenv("ISUCONP_DB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("ISUCONP_DB_PORT")
	if port == "" {
		port = "3306"
	}
	_, err := strconv.Atoi(port)
	if err != nil {
		log.Fatalf("Failed to read DB port number from an environment variable ISUCONP_DB_PORT.\nError: %s", err.Error())
	}
	user := os.Getenv("ISUCONP_DB_USER")
	if user == "" {
		user = "root"
	}
	password := os.Getenv("ISUCONP_DB_PASSWORD")
	dbname := os.Getenv("ISUCONP_DB_NAME")
	if dbname == "" {
		dbname = "isuconp"
	}

	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%s", host, port)
	cfg.DBName = dbname
	cfg.Params = map[string]string{
		"charset": "utf8mb4",
	}
	cfg.ParseTime = true
	cfg.Loc = time.Local
	// クライアント側でプレースホルダを展開し、毎クエリの PREPARE 往復(ADMIN PREPARE)を無くす
	cfg.InterpolateParams = true
	dsn := cfg.FormatDSN()

	db, err = sqlx.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %s.", err.Error())
	}
	defer db.Close()

	r := chi.NewRouter()

	r.Get("/initialize", getInitialize)
	r.Get("/login", getLogin)
	r.Post("/login", postLogin)
	r.Get("/register", getRegister)
	r.Post("/register", postRegister)
	r.Get("/logout", getLogout)
	r.Get("/", getIndex)
	r.Get("/posts", getPosts)
	r.Get("/posts/{id}", getPostsID)
	r.Post("/", postIndex)
	r.Get("/image/{id}.{ext}", getImage)
	r.Post("/comment", postComment)
	r.Get("/admin/banned", getAdminBanned)
	r.Post("/admin/banned", postAdminBanned)
	r.Get(`/@{accountName:[0-9a-zA-Z_]+}`, getAccountName)
	r.Mount("/", http.FileServer(http.Dir("../public")))

	// pprof（計測用・localhostのみ。本番URLには影響しない）
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.Fatal(http.ListenAndServe(":8080", r))
}
