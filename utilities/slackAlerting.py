"""Program to create slack payloads"""

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def slackAlert(inchan, slack_token, payload):
    """Method to send slack alert to channel with payload"""
    client = WebClient(token=slack_token)

    try:
        client.chat_postMessage(channel=inchan, blocks=payload)
        # assert response["message"]["text"] == payload
        return None
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        return f"Got an error: {e.response['error']}"


def header_block(h_in):
    """Method to create header block for slack message"""
    h_out_p1 = """
    {
        "type": "rich_text",
        "elements": [
            {
                "type": "rich_text_section",
                "elements": [
                    {
                        "type": "text",
    """
    h_out_p2 = f"""
                        "text": "{h_in}:",
    """
    h_out_p3 = """      "style": {
                            "bold": true,
                            "italic": true
                        }
                    }
                ]
            }
        ]
    }
    """
    h_out = h_out_p1 + h_out_p2 + h_out_p3
    return h_out


def section_block(s_in, l_text, link):
    """Method to create section block for slack message"""
    s_in_p1 = """
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
    """
    s_in_p2 = f"""
            "text": "{s_in}"
    """
    s_in_p3 = """
    },
        "accessory": {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "Plot",
                "emoji": true
            },
    """
    s_in_p4 = f"""
            "value": "{l_text}",
            "url": "{link}",
    """
    s_in_p5 = """
    "action_id": "button-action"
        }
    }
    """
    s_out = s_in_p1 + s_in_p2 + s_in_p3 + s_in_p4 + s_in_p5
    return s_out


def compilePayloadText(payload_build, s3_url):
    """Method to compile payload text for slack message"""
    msg = ""
    c = 0
    for value in payload_build.values():
        for directory, flags in value.items():
            dir_formatted = header_block(directory.strip("'"))
            flags_formatted = ""
            for f in flags:
                element = f[0][0]
                if flags.index(f) > 0:
                    flags_formatted = (
                        flags_formatted
                        + ",\n"
                        + section_block(
                            element,
                            "Plot",
                            f"{s3_url}/{f[1]}.html",
                        )
                    )
                else:
                    flags_formatted = section_block(
                        element,
                        "Plot",
                        f"{s3_url}/{f[1]}.html",
                    )
            if c == 0:
                msg = (
                    msg
                    + f"""
                {dir_formatted},
                {flags_formatted}
                """
                )
                c += 1
            else:
                msg = (
                    msg
                    + f""",
                {dir_formatted},
                {flags_formatted}
                """
                )

    return msg
