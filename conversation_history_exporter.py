import os
import time
import logging
import pandas as pd
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler


def convert_to_message_view_model(message):
    if message is None:
        return
    timestamp = float(message.get("ts"))
    dt =  datetime.fromtimestamp(timestamp)
    return {
        "ts": message.get("ts"),
        "thread_ts": message.get("thread_ts"),
        "datetime": dt,
        "user": message.get("user"),
        "text": message.get("text")
    }

def batch_convert_to_message_view_model(messages):
    return [convert_to_message_view_model(message) for message in messages]
def get_next_cursor(response):
    if response.get("response_metadata") is None or response.get("response_metadata").get("next_cursor") is None:
        return None
    else: 
        next_cursor = response.get("response_metadata").get("next_cursor")
        logging.info("next_cursor: " + next_cursor)
        return next_cursor

def process(response,file_name,ammend=False):
    msgArray = []
    messages = response.get("messages")
    for message in messages:
        logging.info("ts: " + message["ts"])
        if message.__contains__("subtype") and message.get("subtype") == "thread_broadcast":
            continue
        if message.__contains__("thread_ts"):
            time.sleep(0.5)
            repliesResponse = client.conversations_replies(channel=channel_id, ts=message.get("thread_ts"), limit=message.get("reply_count"))
            reply_messages = repliesResponse.get("messages")
            msgArray.extend(reply_messages) 
        else:
            message.setdefault("thread_ts", message.get("ts"))
            msgArray.append(message)

    if ammend:
        logging.info("ammending csv")
        pd.DataFrame(batch_convert_to_message_view_model(msgArray)).to_csv(file_name, mode='a', header=False)
    else:
        logging.info("writing csv")
        pd.DataFrame(batch_convert_to_message_view_model(msgArray)).to_csv(file_name)
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    export_file = "output/messages_"+ datetime.now().strftime("%Y%m%d%H%M%S")+".csv"
    slack_bot_token = os.environ["SLACK_BOT_TOKEN"]
    client = WebClient(token=slack_bot_token)
    rate_limit_handler = RateLimitErrorRetryHandler(max_retry_count=1)
    client.retry_handlers.append(rate_limit_handler)
    channel_id = os.environ["CHANNEL_ID"]
    limit = 200
    logging.info("starting conversation history export")
    response = client.conversations_history(channel=channel_id, limit=limit)
    logging.info("response: " + str(len(response.get("messages"))))
    has_more = response.get("has_more")
    if has_more:
        cursor = get_next_cursor(response)
    process(response, file_name=export_file)

    while has_more:
        response = client.conversations_history(channel=channel_id, limit=limit, cursor=cursor)
        logging.info("response: " + str(len(response.get("messages"))))
        has_more = response.get("has_more")
        if has_more:
            cursor = get_next_cursor(response)
        process(response,ammend=True, file_name=export_file)
    
    logging.info("No more messages to export.")