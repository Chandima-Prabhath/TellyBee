import pytest
from pydantic import ValidationError

from app.config import Settings

VALID_TOKEN = "1234567890:AAEhBOWe4ifRhnZ2dPq1fDz0r9RYmc8V4zQ"


# ── Env-var / .env-file sources ─────────────────────────────────────────────
# pydantic-settings JSON-decodes complex fields before validators run unless
# the field opts out via NoDecode. These tests pin the opt-out: empty strings,
# CSV and JSON lists must all be accepted from real env vars / .env files
# (docker-compose `env_file:` injects .env values as container env vars).


def test_env_empty_values_use_defaults(monkeypatch):
    monkeypatch.setenv("ADMIN_IDS", "")
    monkeypatch.setenv("ALLOWED_USER_IDS", "")
    monkeypatch.setenv("ALLOWED_TYPES", "")
    monkeypatch.setenv("BLOCKED_TYPES", "")
    s = Settings(bot_token=VALID_TOKEN)
    assert s.admin_ids == set()
    assert s.allowed_user_ids == set()
    assert s.allowed_types == set()
    assert s.blocked_types == set()
    assert s.open_access is True


def test_env_csv_values(monkeypatch):
    monkeypatch.setenv("ADMIN_IDS", "111, 222")
    monkeypatch.setenv("ALLOWED_TYPES", " photo , video ")
    monkeypatch.setenv("BLOCKED_TYPES", "sticker")
    monkeypatch.setenv("ALLOWED_USER_IDS", "333")
    s = Settings(bot_token=VALID_TOKEN)
    assert s.admin_ids == {111, 222}
    assert s.allowed_types == {"photo", "video"}
    assert s.blocked_types == {"sticker"}
    assert s.allowed_user_ids == {333}
    assert s.open_access is False


def test_env_json_list_values(monkeypatch):
    monkeypatch.setenv("ALLOWED_TYPES", '["photo","video"]')
    monkeypatch.setenv("ALLOWED_USER_IDS", "[111, 222]")
    monkeypatch.setenv("ADMIN_IDS", "[333]")
    s = Settings(bot_token=VALID_TOKEN)
    assert s.allowed_types == {"photo", "video"}
    assert s.allowed_user_ids == {111, 222}
    assert s.admin_ids == {333}


def test_env_invalid_json_list_rejected(monkeypatch):
    monkeypatch.setenv("ALLOWED_TYPES", '["photo",')
    with pytest.raises(ValidationError):
        Settings(bot_token=VALID_TOKEN)


def test_dotenv_file_accepts_empty_and_csv(tmp_path, monkeypatch):
    """The shipped .env.example (copied to .env) must parse as-is."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                f"BOT_TOKEN={VALID_TOKEN}",
                "ADMIN_IDS=11111111",
                "ALLOWED_USER_IDS=",
                "ALLOWED_TYPES=",
                "BLOCKED_TYPES=",
                "MAX_FILE_SIZE=20MB",
                "DOWNLOAD_CONCURRENCY=4",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    s = Settings()
    assert s.bot_token == VALID_TOKEN
    assert s.admin_ids == {11111111}
    assert s.allowed_user_ids == set()
    assert s.allowed_types == set()
    assert s.blocked_types == set()
    assert s.max_file_size == 20 * 1024**2
    assert s.download_concurrency == 4


def test_valid_settings(tmp_path):
    s = Settings(
        bot_token=VALID_TOKEN,
        admin_ids="111, 222",
        allowed_types="photo,video",
        max_file_size="20MB",
        download_dir=tmp_path / "dl",
    )
    assert s.admin_ids == {111, 222}
    assert s.allowed_types == {"photo", "video"}
    assert s.max_file_size == 20 * 1024**2
    assert s.open_access is False
    assert s.polling_mode is True


def test_open_access_default():
    s = Settings(bot_token=VALID_TOKEN)
    assert s.open_access is True


def test_bad_token_rejected():
    with pytest.raises(ValueError):
        Settings(bot_token="not-a-token")


def test_bad_media_type_rejected():
    with pytest.raises(ValueError):
        Settings(bot_token=VALID_TOKEN, allowed_types="photo,gadget")


def test_conflicting_type_filters_rejected():
    with pytest.raises(ValueError):
        Settings(bot_token=VALID_TOKEN, allowed_types="photo", blocked_types="photo")


def test_bad_template_token_rejected():
    with pytest.raises(ValueError):
        Settings(bot_token=VALID_TOKEN, file_template="{user}/{nonsense}")


def test_template_traversal_rejected():
    with pytest.raises(ValueError):
        Settings(bot_token=VALID_TOKEN, file_template="../etc/{type}")


def test_webhook_url_switches_mode(tmp_path):
    s = Settings(
        bot_token=VALID_TOKEN,
        webhook_url="https://example.com/tellybee",
    )
    assert s.polling_mode is False
    assert s.webhook_listening is True


def test_webhook_without_url_rejected():
    with pytest.raises(ValueError):
        Settings(bot_token=VALID_TOKEN, polling_mode=False)


def test_size_strings():
    s = Settings(bot_token=VALID_TOKEN, chunk_size="256KB", progress_min_bytes="2MB")
    assert s.chunk_size == 256 * 1024
    assert s.progress_min_bytes == 2 * 1024**2


def test_concurrency_bounds():
    with pytest.raises(ValueError):
        Settings(bot_token=VALID_TOKEN, download_concurrency=0)
    with pytest.raises(ValueError):
        Settings(bot_token=VALID_TOKEN, download_concurrency=99)


def test_summary_masks_token():
    s = Settings(bot_token=VALID_TOKEN)
    summary = s.summary()
    assert VALID_TOKEN not in summary
    assert "token=set" in summary


# ── Mode-aware MAX_FILE_SIZE caps (0.2.0) ───────────────────────────────────
# Local Bot API downloads have NO server-side size limit (the 20MB check is
# only enforced when NOT in --local mode); the cloud Bot API caps at 20MB.


def test_local_mode_allows_up_to_4gb():
    s = Settings(bot_token=VALID_TOKEN, api_base="http://botapi:8081", max_file_size="4000MB")
    assert s.max_file_size == 4000 * 1024**2


def test_local_mode_rejects_above_4gb():
    with pytest.raises(ValidationError, match="4000MB"):
        Settings(bot_token=VALID_TOKEN, api_base="http://botapi:8081", max_file_size="4100MB")


def test_cloud_mode_rejects_above_20mb_with_remedy():
    with pytest.raises(ValidationError, match="API_BASE"):
        Settings(bot_token=VALID_TOKEN, max_file_size="2000MB")


def test_cloud_mode_accepts_20mb():
    s = Settings(bot_token=VALID_TOKEN, max_file_size="20MB")
    assert s.max_file_size == 20 * 1024**2


def test_getfile_timeout_bounds():
    with pytest.raises(ValidationError, match="GETFILE_TIMEOUT"):
        Settings(bot_token=VALID_TOKEN, api_base="http://botapi:8081", getfile_timeout=30)
    with pytest.raises(ValidationError, match="GETFILE_TIMEOUT"):
        Settings(bot_token=VALID_TOKEN, api_base="http://botapi:8081", getfile_timeout=100000)
