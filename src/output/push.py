"""独立推送模块 — 飞书 / 钉钉 webhook"""

import json

import requests

from src.config import PUSH


def _send_feishu(content: str) -> bool:
    """发送飞书 Markdown 卡片消息"""
    url = PUSH["feishu_webhook_url"]
    if not url:
        print("[push] 飞书 webhook 未配置，跳过")
        return False

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": "每日投资决策简报"},
                "template": "blue",
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": content,
                }
            ],
        },
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == 0 or data.get("StatusCode") == 0:
                print("[push] 飞书推送成功")
                return True
            print(f"[push] 飞书返回错误: {data}")
            return False
        print(f"[push] 飞书 HTTP {resp.status_code}")
        return False
    except Exception as e:
        print(f"[push] 飞书推送异常: {e}")
        return False


def _send_dingtalk(content: str) -> bool:
    """发送钉钉 Markdown 消息"""
    url = PUSH["dingtalk_webhook_url"]
    if not url:
        print("[push] 钉钉 webhook 未配置，跳过")
        return False

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": "每日投资决策简报",
            "text": content,
        },
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("errcode") == 0:
                print("[push] 钉钉推送成功")
                return True
            print(f"[push] 钉钉返回错误: {data}")
            return False
        print(f"[push] 钉钉 HTTP {resp.status_code}")
        return False
    except Exception as e:
        print(f"[push] 钉钉推送异常: {e}")
        return False


def push_briefing(content: str) -> dict[str, bool]:
    """推送简报到所有已配置的渠道"""
    results = {}

    if PUSH["feishu_webhook_url"]:
        results["feishu"] = _send_feishu(content)

    if PUSH["dingtalk_webhook_url"]:
        results["dingtalk"] = _send_dingtalk(content)

    if not results:
        print("[push] 未配置任何推送渠道，请设置环境变量 FEISHU_WEBHOOK_URL 或 DINGTALK_WEBHOOK_URL")

    return results
