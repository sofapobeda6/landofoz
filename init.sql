CREATE TABLE IF NOT EXISTS videos (
    id TEXT PRIMARY KEY,
    creator_id TEXT NOT NULL,
    video_created_at TIMESTAMPTZ NOT NULL,
    views_count BIGINT DEFAULT 0,
    likes_count BIGINT DEFAULT 0,
    comments_count BIGINT DEFAULT 0,
    reports_count BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS video_snapshots (
    id TEXT PRIMARY KEY,
    video_id TEXT REFERENCES videos(id) ON DELETE CASCADE,
    views_count BIGINT DEFAULT 0,
    likes_count BIGINT DEFAULT 0,
    comments_count BIGINT DEFAULT 0,
    reports_count BIGINT DEFAULT 0,
    delta_views_count BIGINT DEFAULT 0,
    delta_likes_count BIGINT DEFAULT 0,
    delta_comments_count BIGINT DEFAULT 0,
    delta_reports_count BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_videos_creator ON videos(creator_id);
CREATE INDEX IF NOT EXISTS idx_videos_created ON videos(video_created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_video ON video_snapshots(video_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_created ON video_snapshots(created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_delta ON video_snapshots(delta_views_count) WHERE delta_views_count > 0;