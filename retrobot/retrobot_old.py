from __future__ import print_function

import os
import time
import re
import datetime as dt

import pandas as pd
from slackclient import SlackClient


slack_client = SlackClient(os.environ.get('SLACK_BOT_TOKEN'))

def get_bot_id(bot_name):
    """
        Gets bot id.
    """
    api_call = slack_client.api_call("users.list")
    if api_call.get('ok'):
        # retrieve all users so we can find our bot
        users = api_call.get('members')
        for user in users:
            if 'name' in user and user.get('name') == bot_name:
                return str(user.get('id'))
    else:
        print("No bot named: " + BOT_NAME)


BOT_NAME = os.environ.get('BOT_NAME')
BOT_ID = get_bot_id(os.environ.get('BOT_NAME'))

AT_BOT = "<@" + BOT_ID + ">"
READ_WEBSOCKET_DELAY = 1 # 1 second delay between reading from firehose
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
DIR_PATH = DIR_PATH + '/.tmp/'

def get_message_reactions(channel, timestamp):
    """
        Update dataframe with reaction data.
    """
    response = slack_client.api_call("reactions.get", channel=channel, timestamp=timestamp)
    if not response['ok']:
        return 0
    if 'reactions' in response['message']:
        return len(response['message']['reactions'])
    return 0

def update_reactions(dataframe):
    """
        Take a dataframe and update reactions on the dataframe.
    """
    dataframe['reactions'] = dataframe.apply(
        lambda row: get_message_reactions(row['channel'], row['ts']), axis=1
    )
    return dataframe

def feedback_looper(dataframe):
    """
        Creates the feedback text for message froma a dataframe.
    """
    items = ['{} {} --> {} -- Reactions: {}\n'.format(
        row['time'].strftime('%Y-%m-%d'),
        row['user'],
        row['command'],
        row['reactions']
        ) for _, row in dataframe.iterrows()]

    return ''.join(items)

def summarize_feedback(data, command, channel):
    """
        Takes a summarize command, channel and state. Returns a message with summary.
    """
    dates = re.findall(r'\d{4}-\d{2}-\d{2}', command)
    if len(dates) != 2:
        #Send message about too many or too few dates
        return 'Too many or too few dates.'

    if dates[0] > dates[1]:
        #Send message about bad dates
        return '{start} is greater than {end}'.format(start=dates[0], end=dates[1])

    #find feedback between dates
    temp = data[(data.time > dates[0]) & (data.time <= dates[1]) & (data.channel == channel)]
    #organize them by category
    start_df = temp[temp.category == 'start'].sort_values(by='reactions', ascending=False)
    stop_df = temp[temp.category == 'stop'].sort_values(by='reactions', ascending=False)
    continue_df = temp[temp.category == 'continue'].sort_values(by='reactions', ascending=False)
    kudo_df = temp[temp.category == 'kudo'].sort_values(by='reactions', ascending=False)

    if start_df.empty and stop_df.empty and continue_df.empty and kudo_df.empty:
        return "No feedback during this period."

    message = ("Summary of Feedback from {start} to {end}\n"
                "*Starts*\n{starts}"
                "*Stops*\n{stops}"
                "*Continues*\n{continues}"
                "*Kudos*\n{kudos}").format(
                    start=dates[0],
                    end=dates[1],
                    starts=feedback_looper(start_df),
                    stops=feedback_looper(stop_df),
                    continues=feedback_looper(continue_df),
                    kudos=feedback_looper(kudo_df)
                    )
    return message

def return_message_df(category, user, command, channel, ts, columns):
    """
        Takes data and returns a dataframe.
    """
    temp = pd.DataFrame(
        [[
            category,
            user['user']['name'],
            channel,
            command,
            dt.datetime.now(),
            ts,
            None
        ]],
        columns=columns,
        )
    return temp

def note_item(channel, ts):
    """
        Takes a timestamp and channel to note a retro item was recieved.
    """
    slack_client.api_call(
        "reactions.add",
        channel=channel,
        timestamp=ts,
        name="white_check_mark"
    )

def handle_command(data, command, channel, user, ts, columns):
    """
        Receives commands directed at the bot and determines if they
        are valid commands. If so, then acts on the commands. If not,
        returns back what it needs for clarification.
    """
    user = slack_client.api_call("users.info", user=user)

    if command.startswith('start'):
        temp = return_message_df('start', user, command, channel, ts, columns)
        data = data.append(temp, ignore_index=True)
        note_item(channel, ts)

    elif command.startswith('stop'):
        temp = return_message_df('stop', user, command, channel, ts, columns)
        data = data.append(temp, ignore_index=True)
        note_item(channel, ts)

    elif command.startswith('continue'):
        temp = return_message_df('continue', user, command, channel, ts, columns)
        data = data.append(temp, ignore_index=True)
        note_item(channel, ts)

    elif command.startswith('kudo'):
        temp = return_message_df('kudo', user, command, channel, ts, columns)
        data = data.append(temp, ignore_index=True)
        note_item(channel, ts)

    elif command.startswith('summarize'):
        data = update_reactions(data)
        message = summarize_feedback(data, command, channel)
        slack_client.api_call(
            "chat.postMessage",
            channel=channel,
            text=message,

            )
    else:
        pass # add a failure response

    return data

def parse_slack_message_output(slack_rtm_output):
    """
        The Slack Real Time Messaging API is an events firehose.
        this parsing function returns None unless a message is
        directed at the Bot, based on its ID.
    """
    output_list = slack_rtm_output
    if output_list and len(output_list) > 0:
        for output in output_list:
            if output and 'text' in output and AT_BOT in output['text']:
                # return text after the @ mention, whitespace removed
                return output['text'].split(AT_BOT)[1].strip().lower(), \
                       output['channel'], \
                       output['user'], \
                       output['ts']
    return None, None, None, None

def main():
    '''
        Intializes the application.
    '''

    # create or re-intialize dataframe
    columns=['category', 'user', 'channel', 'command', 'time', 'ts', 'reactions']

    if os.path.isfile(DIR_PATH+'retrobot.csv'):
        data = pd.read_csv(DIR_PATH+'retrobot.csv', usecols=columns)

    else:
        data = pd.DataFrame(columns=columns)

    #convert to datetime object
    data['time'] = pd.to_datetime(data['time'])

    if slack_client.rtm_connect():
        print ("StarterBot connected and running!")
        while True:
            command, channel, user, ts = parse_slack_message_output(slack_client.rtm_read())
            if command and channel:
                data = handle_command(data, command, channel, user, ts, columns)
                data.to_csv((DIR_PATH+'retrobot.csv'))
                print(data)

            time.sleep(READ_WEBSOCKET_DELAY)
    else:
        print("Connection failed. Invalid Slack token or bot ID?")

if __name__ == "__main__":
    main()
