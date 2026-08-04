from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
from tests.gateway.test_whatsapp_formatting import _AsyncCM, _make_adapter


class TestWhatsAppNativeFormatting:

    def test_invisible_unicode_prefixes_are_sanitized(self):
        adapter = _make_adapter()

        assert adapter.format_message("\u2060\u202ftext") == " text"


@pytest.mark.asyncio
async def test_send_location_posts_to_bridge_location_endpoint():
    adapter = _make_adapter()
    resp = MagicMock(status=200)
    resp.json = AsyncMock(return_value={"success": True, "messageId": "loc-msg"})
    adapter._http_session.post = MagicMock(return_value=_AsyncCM(resp))

    result = await adapter.send_location(
        "15551234567",
        41.015,
        28.979,
        name="HQ",
        address="Example Street",
    )

    assert result.success
    assert result.message_id == "loc-msg"
    call = adapter._http_session.post.call_args
    assert call.args[0] == "http://127.0.0.1:3000/send-location"
    assert call.kwargs["json"] == {
        "chatId": "15551234567@s.whatsapp.net",
        "latitude": 41.015,
        "longitude": 28.979,
        "name": "HQ",
        "address": "Example Street",
    }


@pytest.mark.asyncio
async def test_send_tracks_text_chunk_message_ids_in_snake_case_raw_response():
    adapter = _make_adapter()
    first = MagicMock(status=200)
    first.json = AsyncMock(return_value={"success": True, "messageId": "msg-1"})
    second = MagicMock(status=200)
    second.json = AsyncMock(return_value={"success": True, "messageId": "msg-2"})
    adapter._http_session.post = MagicMock(side_effect=[_AsyncCM(first), _AsyncCM(second)])

    result = await adapter.send("15551234567", "x" * (adapter.MAX_MESSAGE_LENGTH + 100))

    assert result.success
    assert result.message_id == "msg-2"
    assert result.continuation_message_ids == ("msg-1",)
    assert result.raw_response["message_ids"] == ["msg-1", "msg-2"]
    assert "messageIds" not in result.raw_response


@pytest.mark.asyncio
async def test_whatsapp_reply_context_is_structured_not_prerendered():
    adapter = WhatsAppAdapter(
        PlatformConfig(
            enabled=True,
            extra={"session_name": "test", "dm_policy": "allowlist", "allow_from": ["*"]},
        )
    )

    event = await adapter._build_message_event(
        {
            "body": "what do you see here?",
            "chatId": "15551234567@s.whatsapp.net",
            "chatName": "Example Chat",
            "senderId": "15551234567@s.whatsapp.net",
            "senderName": "Example User",
            "isGroup": False,
            "hasQuotedMessage": True,
            "quotedText": "the gateway should not inject reply context twice",
            "quotedMessageId": "quoted-123",
        }
    )

    assert event is not None
    assert event.text == "what do you see here?"
    assert event.reply_to_message_id == "quoted-123"
    assert event.reply_to_text == "the gateway should not inject reply context twice"
    assert not event.text.startswith("[Replying to:")


@pytest.mark.asyncio
async def test_whatsapp_processing_start_and_complete_reactions():
    adapter = _make_adapter()
    resp = MagicMock(status=200)
    resp.json = AsyncMock(return_value={"success": True, "messageId": "status-msg-1"})
    adapter._http_session.post = MagicMock(return_value=_AsyncCM(resp))

    event = MagicMock()
    event.source.chat_id = "15551234567@s.whatsapp.net"
    event.message_id = "user-msg-123"

    # 1. Test processing start (should send ⏳ status message bubble; reactions disabled per config)
    await adapter.on_processing_start(event)

    # Verify status bubble post call was made (1 call)
    assert adapter._http_session.post.call_count == 1
    assert adapter._whatsapp_status_messages.get("15551234567@s.whatsapp.net") == "status-msg-1"

    # 2. Test processing complete with success (pops mapping; does not delete message or send result reaction)
    from gateway.platforms.base import ProcessingOutcome
    await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)

    # Post call count remains 1 and status mapping is cleaned up
    assert adapter._http_session.post.call_count == 1
    assert "15551234567@s.whatsapp.net" not in adapter._whatsapp_status_messages


@pytest.mark.asyncio
async def test_send_image_file_delivers_natively_with_caption(tmp_path):
    adapter = _make_adapter()
    img_file = tmp_path / "chart.png"
    img_file.write_bytes(b"\x89PNG\r\n\x1a\nfake_image_data")

    resp = MagicMock(status=200)
    resp.json = AsyncMock(return_value={"success": True, "messageId": "img-msg-1"})
    adapter._http_session.post = MagicMock(return_value=_AsyncCM(resp))

    res = await adapter.send_image_file(
        "15551234567",
        str(img_file),
        caption="Monthly Chart",
    )

    assert res.success
    assert res.message_id == "img-msg-1"
    call = adapter._http_session.post.call_args
    assert call.args[0] == "http://127.0.0.1:3000/send-media"
    assert call.kwargs["json"] == {
        "chatId": "15551234567@s.whatsapp.net",
        "filePath": str(img_file),
        "mediaType": "image",
        "caption": "Monthly Chart",
    }


@pytest.mark.asyncio
async def test_send_image_file_fallback_on_failure(tmp_path):
    adapter = _make_adapter()
    img_file = tmp_path / "chart.png"
    img_file.write_bytes(b"fake")

    fail_resp = MagicMock(status=500)
    fail_resp.text = AsyncMock(return_value="Bridge internal error")
    ok_resp = MagicMock(status=200)
    ok_resp.json = AsyncMock(return_value={"success": True, "messageId": "text-fallback-1"})

    adapter._http_session.post = MagicMock(side_effect=[_AsyncCM(fail_resp), _AsyncCM(ok_resp)])

    res = await adapter.send_image_file(
        "15551234567",
        str(img_file),
        caption="Failed Image",
    )

    # Bridge failed (500), should fall back safely to text send
    assert res.success
    assert res.message_id == "text-fallback-1"
    calls = adapter._http_session.post.call_args_list
    assert len(calls) == 2
    assert calls[0].args[0] == "http://127.0.0.1:3000/send-media"
    assert calls[1].args[0] == "http://127.0.0.1:3000/send"
    assert "Couldn't deliver the image attachment" in calls[1].kwargs["json"]["message"]


@pytest.mark.asyncio
async def test_send_image_local_uri_delivers_natively(tmp_path):
    adapter = _make_adapter()
    img_file = tmp_path / "photo.jpg"
    img_file.write_bytes(b"fake_jpeg")

    resp = MagicMock(status=200)
    resp.json = AsyncMock(return_value={"success": True, "messageId": "uri-img-1"})
    adapter._http_session.post = MagicMock(return_value=_AsyncCM(resp))

    file_uri = f"file://{img_file}"
    res = await adapter.send_image(
        "15551234567",
        file_uri,
        caption="Local Photo",
    )

    assert res.success
    assert res.message_id == "uri-img-1"
    call = adapter._http_session.post.call_args
    assert call.args[0] == "http://127.0.0.1:3000/send-media"
    assert call.kwargs["json"]["filePath"] == str(img_file)
    assert call.kwargs["json"]["mediaType"] == "image"
