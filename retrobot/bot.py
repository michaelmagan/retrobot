from __future__ import print_function

import time
import os
import re
import datetime as dt

import pandas as pd

from slackclient import SlackClient

"""This module contains a bot object that can be used for slack retrobot."""


class Bot(object):
    """
        This returns a bot object.
    """

    def __init__(self, api_key, bot_name, time_delay=1):
        self.api_key = api_key
        self.bot_name = bot_name
        self.time_delay = time_delay
        self.columns = ['category', 'user', 'channel', 'command', 'time', 'timestamp', 'reactions']
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.state_file_path = dir_path + '/.tmp/' + 'bot_state.csv'


        self.slack_client = SlackClient(self.api_key)

        self.get_bot_id()
        self.deserialize_state()

    def get_bot_id(self):
        """
            Finds bot_id and bot_at attributes.
        """
        api_result = self.slack_client.api_call("users.list")
        if api_result.get("ok"):
            users = api_result.get("members")
            for user in users:
                if 'name' in user and user.get('name') == self.bot_name:
                    self.bot_id = user.get('id')
                    self.bot_at = "<@" + self.bot_id + ">"

    def deserialize_state(self):
        """
            Uploads the csv.
        """
        if os.path.isfile(self.state_file_path):
            self.dataframe = pd.read_csv(self.state_file_path, usecols=self.columns)
        else:
            self.dataframe = pd.DataFrame(columns=self.columns)

        #convert to datetime object
        self.dataframe['time'] = pd.to_datetime(self.dataframe['time'])

    def listen(self):
        """
            Listen for @ bot messages.
        """
        if self.slack_client.rtm_connect():
            print ("StarterBot connected and running!")
            while True:
                command, channel, user, timestamp = self.parse_slack_message_output(self.slack_client.rtm_read())
                if command and channel:
                    self.command_handler(command, channel, user, timestamp)
                    time.sleep(self.time_delay)
        else:
            print("Connection failed. Invalid Slack token or bot ID?")

    def parse_slack_message_output(self, slack_rtm_output):
        """
            The Slack Real Time Messaging API is an events firehose.
            this parsing function returns None unless a message is
            directed at the Bot, based on its ID.
        """
        output_list = slack_rtm_output
        if output_list and len(output_list) > 0:
            for output in output_list:
                if output and 'text' in output and self.bot_at in output['text']:
                    # return text after the @ mention, whitespace removed
                    return output['text'].split(self.bot_at)[1].strip().lower(), \
                        output['channel'], \
                        output['user'], \
                        output['ts']
        return None, None, None, None

    def note_item(self, channel, timestamp):
        """
            Takes a timestamp and channel to note a retro item was recieved.
        """
        self.slack_client.api_call(
            "reactions.add",
            channel=channel,
            timestamp=timestamp,
            name="white_check_mark"
        )

    def command_handler(self, command, channel, user, timestamp):
        """
            This should handel commands.
        """
        user = self.slack_client.api_call('users.info', user=user)
        
        if command.startswith('start'):
            self.store_feedback('start', user, command, channel, timestamp)
            self.note_item(channel, timestamp)

        elif command.startswith('stop'):
            self.store_feedback('stop', user, command, channel,timestamp)
            self.note_item(channel, timestamp)

        elif command.startswith('continue'):
            self.store_feedback('continue', user, command, channel, timestamp)
            self.note_item(channel, timestamp)

        elif command.startswith('kudo'):
            self.store_feedback('kudo', user, command, channel, timestamp)
            self.note_item(channel, timestamp)

        elif command.startswith('summarize'):
            self.dataframe = self.update_reactions(self.dataframe)
            self.save_state()
            message = self.summarize_feedback(self.dataframe, command, channel)
            self.slack_client.api_call(
                "chat.postMessage",
                channel=channel,
                text=message,

                )

        else:
            pass # show commands

    def store_feedback(self,category, user, command, channel, timestamp):
        """
            Store feedback.
        """

        row = pd.DataFrame(
        [[
            category,
            user['user']['name'],
            channel,
            command,
            dt.datetime.now(),
            timestamp,
            None
        ]],
        columns=self.columns,
        )
        self.dataframe = self.dataframe.append(row, ignore_index=True)
        self.save_state()

    def summarize_feedback(self, data, command, channel):
        """
            Takes a summarize command, channel and state. Returns a message with summary.
        """
        dates = re.findall(r'\d{4}-\d{2}-\d{2}', command)
        if len(dates) != 2:
            #Send message about too many or too few dates
            return 'Too many or too few complete dates.'

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
                        starts=self.feedback_looper(start_df),
                        stops=self.feedback_looper(stop_df),
                        continues=self.feedback_looper(continue_df),
                        kudos=self.feedback_looper(kudo_df)
                        )
        return message

    def get_message_reactions(self, channel, timestamp):
        """
        Update dataframe with reaction data.
        """
        response = self.slack_client.api_call("reactions.get", channel=channel, timestamp=timestamp)
        if not response['ok']:
            return 0
        if 'reactions' in response['message']:
            return len(response['message']['reactions'])
        return 0

  
    def update_reactions(self, dataframe):
        """
        Take a dataframe and update reactions on the dataframe.
        """
        dataframe['reactions'] = dataframe.apply(
            lambda row: self.get_message_reactions(row['channel'], row['timestamp']), axis=1
        )

        return dataframe

    def save_state(self):
        """
            Saves data to disk.
        """
        self.dataframe.to_csv((self.state_file_path))
        print(self.dataframe)

    def feedback_looper(self, dataframe):
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
