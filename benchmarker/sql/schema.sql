-- benchmarker/userdata/load.rbから読み込まれる

DROP TABLE IF EXISTS users;
CREATE TABLE users (
  `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `account_name` varchar(64) NOT NULL UNIQUE,
  `passhash` varchar(128) NOT NULL, -- SHA2 512 non-binary (hex)
  `authority` tinyint(1) NOT NULL DEFAULT 0,
  `del_flg` tinyint(1) NOT NULL DEFAULT 0,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
) DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS posts;
CREATE TABLE posts (
  `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` int NOT NULL,
  `mime` varchar(64) NOT NULL,
  `imgdata` mediumblob NOT NULL,
  `body` text NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  -- 第3回: GET / / getPosts の created_at 降順 LIMIT 用
  KEY `idx_created_at` (`created_at`),
  -- 第4回: getAccountName の posts(user_id) 取得用複合索引
  --        (単純な posts(user_id) は GET / の JOIN プランを悪化させるため複合 + FORCE INDEX で運用)
  KEY `idx_user_created` (`user_id`, `created_at`)
) DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS comments;
CREATE TABLE comments (
  `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `post_id` int NOT NULL,
  `user_id` int NOT NULL,
  `comment` text NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  -- 第1回: makePosts のコメント取得 / コメント数集計の post_id 全件走査を解消
  KEY `idx_post_id_created` (`post_id`, `created_at`),
  -- 第8回: getAccountName の SELECT COUNT(*) WHERE user_id=? の全件走査を解消(covering index)
  KEY `idx_user_id` (`user_id`)
) DEFAULT CHARSET=utf8mb4;
