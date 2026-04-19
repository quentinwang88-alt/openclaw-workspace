#!/usr/bin/env python3
"""发布适配器测试。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import tempfile
import sys
import unittest
from unittest.mock import Mock, patch

import requests


TESTS_DIR = Path(__file__).resolve().parent
SKILL_DIR = TESTS_DIR.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from app.publishers import GeeLarkPublishAdapter  # noqa: E402


class GeeLarkPublishAdapterTest(unittest.TestCase):
    @patch("app.publishers.requests.request")
    def test_create_scheduled_task_uses_configurable_fields(self, mock_request: Mock) -> None:
        mock_response = Mock()
        mock_response.json.return_value = {"data": {"task_id": "task-123"}}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        adapter = GeeLarkPublishAdapter(
            token="secret",
            endpoint="https://openapi.geelark.cn/open/v1/task/add",
            plan_name_field="planName",
            remark_field="remark",
            task_type_field="taskType",
            list_field="list",
            task_type_value=1,
            env_id_field="envId",
            video_field="video",
            schedule_at_field="scheduleAt",
            video_desc_field="videoDesc",
            extra_body_json='{"top_level":{"markAI":false},"item":{"needShareLink":true}}',
        )

        task_id = adapter.create_scheduled_task(
            account_id="acc-1",
            video_path="https://demo.geelark.cn/open-upload/demo.mp4",
            title="hello",
            publish_at=datetime(2026, 4, 9, 12, 0, 0),
            script_id="001_M1_M",
            product_id="7498614361651",
        )

        self.assertEqual(task_id, "task-123")
        _, _, kwargs = mock_request.call_args.args[0], mock_request.call_args.args[1], mock_request.call_args.kwargs
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer secret")
        self.assertEqual(
            kwargs["json"],
            {
                "planName": "hello",
                "remark": "001_M1_M",
                "taskType": 1,
                "list": [
                    {
                        "scheduleAt": int(datetime(2026, 4, 9, 12, 0, 0).timestamp()),
                        "envId": "acc-1",
                        "video": "https://demo.geelark.cn/open-upload/demo.mp4",
                        "videoDesc": "hello",
                        "productId": "7498614361651",
                        "needShareLink": True,
                    }
                ],
                "markAI": False,
            },
        )

    @patch("app.publishers.requests.request")
    def test_create_scheduled_task_accepts_task_ids_array_response(self, mock_request: Mock) -> None:
        mock_response = Mock()
        mock_response.json.return_value = {"data": {"taskIds": ["614372922865225911"]}}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        adapter = GeeLarkPublishAdapter(token="secret")
        task_id = adapter.create_scheduled_task(
            account_id="acc-1",
            video_path="https://demo.geelark.cn/open-upload/demo.mp4",
            title="hello",
            publish_at=datetime(2026, 4, 9, 12, 0, 0),
            script_id="001_M1_M",
        )

        self.assertEqual(task_id, "614372922865225911")

    @patch("app.publishers.requests.request")
    def test_create_scheduled_task_uploads_local_file_before_adding_task(self, mock_request: Mock) -> None:
        upload_meta_response = Mock()
        upload_meta_response.json.return_value = {
            "data": {
                "uploadUrl": "https://upload.example.com/put-target",
                "resourceUrl": "https://material-prod.geelark.cn/open-upload/demo.mp4",
            }
        }
        upload_meta_response.raise_for_status.return_value = None

        task_add_response = Mock()
        task_add_response.json.return_value = {"data": {"task_id": "task-456"}}
        task_add_response.raise_for_status.return_value = None
        upload_response = Mock()
        upload_response.raise_for_status.return_value = None

        def request_side_effect(method, url, **kwargs):
            if "upload/getUrl" in url:
                return upload_meta_response
            if method == "PUT":
                return upload_response
            return task_add_response

        mock_request.side_effect = request_side_effect

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / "demo.mp4"
            local_path.write_bytes(b"video-bytes")

            adapter = GeeLarkPublishAdapter(token="secret")
            task_id = adapter.create_scheduled_task(
                account_id="acc-2",
                video_path=str(local_path),
                title="local-video",
                publish_at=datetime(2026, 4, 9, 13, 0, 0),
                script_id="002_M1_M",
            )

        self.assertEqual(task_id, "task-456")
        first_call = mock_request.call_args_list[0]
        second_call = mock_request.call_args_list[1]
        third_call = mock_request.call_args_list[2]
        self.assertEqual(first_call.args[0], "POST")
        self.assertEqual(first_call.kwargs["json"], {"fileType": "mp4"})
        self.assertEqual(second_call.args[0], "PUT")
        self.assertEqual(second_call.args[1], "https://upload.example.com/put-target")
        self.assertEqual(second_call.kwargs["headers"]["Content-Length"], str(len(b"video-bytes")))
        self.assertNotIn("Content-Type", second_call.kwargs["headers"])
        self.assertEqual(third_call.kwargs["json"]["list"][0]["video"], "https://material-prod.geelark.cn/open-upload/demo.mp4")

    @patch("app.publishers.requests.request")
    def test_upload_local_file_keeps_presigned_upload_url_unchanged(self, mock_request: Mock) -> None:
        upload_meta_response = Mock()
        upload_meta_response.json.return_value = {
            "data": {
                "uploadUrl": "https://upload.example.com/open-upload%2Ffolder%2Fdemo.mp4?Expires=123&Signature=abc%3D",
                "resourceUrl": "https://material-prod.geelark.cn/open-upload/demo.mp4",
            }
        }
        upload_meta_response.raise_for_status.return_value = None

        upload_response = Mock()
        upload_response.raise_for_status.return_value = None
        task_add_response = Mock()
        task_add_response.json.return_value = {"data": {"task_id": "task-encoded"}}
        task_add_response.raise_for_status.return_value = None

        mock_request.side_effect = [upload_meta_response, upload_response, task_add_response]

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / "demo.mp4"
            local_path.write_bytes(b"video-bytes")
            adapter = GeeLarkPublishAdapter(token="secret")
            task_id = adapter.create_scheduled_task(
                account_id="acc-2",
                video_path=str(local_path),
                title="local-video",
                publish_at=datetime(2026, 4, 9, 13, 0, 0),
                script_id="002_M1_M",
            )

        self.assertEqual(task_id, "task-encoded")
        put_call = mock_request.call_args_list[1]
        self.assertEqual(
            put_call.args[1],
            "https://upload.example.com/open-upload%2Ffolder%2Fdemo.mp4?Expires=123&Signature=abc%3D",
        )

    @patch("app.publishers.requests.request")
    def test_upload_local_file_forwards_upload_headers_from_meta(self, mock_request: Mock) -> None:
        upload_meta_response = Mock()
        upload_meta_response.json.return_value = {
            "data": {
                "uploadUrl": "https://upload.example.com/put-target",
                "resourceUrl": "https://material-prod.geelark.cn/open-upload/demo.mp4",
                "headers": {
                    "Content-Type": "video/mp4",
                    "x-oss-meta-source": "geelark",
                },
            }
        }
        upload_meta_response.raise_for_status.return_value = None

        upload_response = Mock()
        upload_response.raise_for_status.return_value = None
        task_add_response = Mock()
        task_add_response.json.return_value = {"data": {"task_id": "task-headers"}}
        task_add_response.raise_for_status.return_value = None

        mock_request.side_effect = [upload_meta_response, upload_response, task_add_response]

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / "demo.mp4"
            local_path.write_bytes(b"video-bytes")
            adapter = GeeLarkPublishAdapter(token="secret")
            task_id = adapter.create_scheduled_task(
                account_id="acc-2",
                video_path=str(local_path),
                title="local-video",
                publish_at=datetime(2026, 4, 9, 13, 0, 0),
                script_id="002_M1_M",
            )

        self.assertEqual(task_id, "task-headers")
        put_headers = mock_request.call_args_list[1].kwargs["headers"]
        self.assertEqual(put_headers["Content-Type"], "video/mp4")
        self.assertEqual(put_headers["x-oss-meta-source"], "geelark")
        self.assertEqual(put_headers["Content-Length"], str(len(b"video-bytes")))

    @patch("app.publishers.requests.request")
    def test_upload_local_file_includes_response_body_when_upload_fails(self, mock_request: Mock) -> None:
        upload_meta_response = Mock()
        upload_meta_response.json.return_value = {
            "data": {
                "uploadUrl": "https://upload.example.com/put-target",
                "resourceUrl": "https://material-prod.geelark.cn/open-upload/demo.mp4",
            }
        }
        upload_meta_response.raise_for_status.return_value = None

        upload_error_response = Mock()
        upload_error_response.raise_for_status.side_effect = requests.HTTPError("501 Server Error: Not Implemented")
        upload_error_response.text = "<Error><Code>NotImplemented</Code><Header>Transfer-Encoding</Header></Error>"

        mock_request.side_effect = [upload_meta_response, upload_error_response]

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / "demo.mp4"
            local_path.write_bytes(b"video-bytes")
            adapter = GeeLarkPublishAdapter(token="secret")
            with self.assertRaises(requests.HTTPError) as ctx:
                adapter.create_scheduled_task(
                    account_id="acc-2",
                    video_path=str(local_path),
                    title="local-video",
                    publish_at=datetime(2026, 4, 9, 13, 0, 0),
                    script_id="002_M1_M",
                )

        self.assertIn("GeeLark 上传视频失败", str(ctx.exception))
        self.assertIn("Transfer-Encoding", str(ctx.exception))

    @patch("app.publishers.requests.Session")
    @patch("app.publishers.requests.request")
    def test_upload_local_file_bypasses_env_proxy_for_plain_http_upload_url(self, mock_request: Mock, mock_session_cls: Mock) -> None:
        upload_meta_response = Mock()
        upload_meta_response.json.return_value = {
            "data": {
                "uploadUrl": "http://upload.example.com/put-target",
                "resourceUrl": "https://material-prod.geelark.cn/open-upload/demo.mp4",
            }
        }
        upload_meta_response.raise_for_status.return_value = None
        mock_request.return_value = upload_meta_response

        upload_response = Mock()
        upload_response.raise_for_status.return_value = None
        task_add_response = Mock()
        task_add_response.json.return_value = {"data": {"task_id": "task-789"}}
        task_add_response.raise_for_status.return_value = None

        mock_session = Mock()
        mock_session.request.return_value = upload_response
        mock_session_cls.return_value = mock_session

        def request_side_effect(method, url, **kwargs):
            if "upload/getUrl" in url:
                return upload_meta_response
            return task_add_response

        mock_request.side_effect = request_side_effect

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / "demo.mp4"
            local_path.write_bytes(b"video-bytes")
            adapter = GeeLarkPublishAdapter(token="secret")
            task_id = adapter.create_scheduled_task(
                account_id="acc-2",
                video_path=str(local_path),
                title="local-video",
                publish_at=datetime(2026, 4, 9, 13, 0, 0),
                script_id="002_M1_M",
            )

        self.assertEqual(task_id, "task-789")
        self.assertFalse(mock_session.trust_env)
        mock_session.request.assert_called_once()
        self.assertEqual(mock_session.request.call_args.args[0], "PUT")
        self.assertEqual(mock_session.request.call_args.args[1], "http://upload.example.com/put-target")
        mock_session.close.assert_called_once()

    @patch("app.publishers.requests.request")
    def test_query_task_status_supports_numeric_status_values(self, mock_request: Mock) -> None:
        response = Mock()
        response.json.return_value = {"data": {"status": 3, "published_at": "2026-04-09 12:00:00"}}
        response.raise_for_status.return_value = None
        mock_request.return_value = response

        adapter = GeeLarkPublishAdapter(
            token="secret",
            status_endpoint="https://openapi.geelark.cn/open/v1/task/detail",
            status_task_id_field="taskId",
            status_value_paths="data.status",
            published_at_paths="data.published_at",
            success_values="3,success",
            failure_values="4,failed",
        )

        result = adapter.query_task_status(task_id="task-1", scheduled_for=datetime(2026, 4, 9, 12, 0, 0))
        self.assertEqual(result.state, "success")
        self.assertEqual(result.published_at, "2026-04-09 12:00:00")

    @patch("app.publishers.requests.request")
    def test_query_task_status_uses_ids_array_for_geelark_query(self, mock_request: Mock) -> None:
        response = Mock()
        response.json.return_value = {
            "data": {
                "total": 1,
                "items": [
                    {
                        "id": "task-xyz",
                        "status": 4,
                        "failDesc": "upload failed",
                    }
                ],
            }
        }
        response.raise_for_status.return_value = None
        mock_request.return_value = response

        adapter = GeeLarkPublishAdapter(
            token="secret",
            status_endpoint="https://openapi.geelark.cn/open/v1/task/query",
            status_method="POST",
            status_task_id_field="ids",
            status_value_paths="data.items.0.status",
            error_message_paths="data.items.0.failDesc",
            success_values="3",
            failure_values="4,7",
        )

        result = adapter.query_task_status(task_id="task-xyz", scheduled_for=datetime(2026, 4, 9, 12, 0, 0))
        self.assertEqual(result.state, "failed")
        self.assertEqual(result.error_message, "upload failed")
        self.assertEqual(mock_request.call_args.kwargs["json"], {"ids": ["task-xyz"]})

    @patch("app.publishers.requests.request")
    def test_geelark_request_retries_after_ssl_error(self, mock_request: Mock) -> None:
        success_response = Mock()
        success_response.json.return_value = {"data": {"taskIds": ["614372922865225911"]}}
        success_response.raise_for_status.return_value = None
        mock_request.side_effect = [
            requests.exceptions.SSLError("EOF occurred in violation of protocol"),
            success_response,
        ]

        adapter = GeeLarkPublishAdapter(token="secret", request_max_retries=1, request_retry_backoff_seconds=1)
        task_id = adapter.create_scheduled_task(
            account_id="acc-1",
            video_path="https://demo.geelark.cn/open-upload/demo.mp4",
            title="hello",
            publish_at=datetime(2026, 4, 9, 12, 0, 0),
            script_id="001_M1_M",
        )

        self.assertEqual(task_id, "614372922865225911")
        self.assertEqual(mock_request.call_count, 2)


if __name__ == "__main__":
    unittest.main()
