import pytest

from app.config import Settings

VALID_TOKEN = "1234567890:AAEhBOWe4ifRhnZ2dPq1fDz0r9RYmc8V4zQ"


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
