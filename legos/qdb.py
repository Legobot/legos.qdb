from Legobot.Lego import Lego
from pathlib import Path
import logging
import sqlite3
import random

logger = logging.getLogger(__name__)


class Qdb(Lego):
    def __init__(self, baseplate, lock):
        super().__init__(baseplate, lock)
        db_file = Path('q.db')
        if db_file.exists():
            self.db = sqlite3.connect('q.db')
        else:
            open('q.db', 'a').close()
            self.db = sqlite3.connect('q.db')
        self._create_schema()

    def _create_schema(self):
        create_quotes_table = '''CREATE TABLE IF NOT EXISTS quotes
                                (id INTEGER PRIMARY KEY ASC, quote TEXT,
                                user TEXT, ts DATETIME DEFAULT
                                CURRENT_TIMESTAMP);'''
        self.db.execute(create_quotes_table)
        create_temp_table = '''CREATE TABLE IF NOT EXISTS temp
                              (id INTEGER PRIMARY KEY ASC, quote TEXT,
                              user TEXT, ts DATETIME DEFAULT
                              CURRENT_TIMESTAMP);'''
        self.db.execute(create_temp_table)
        self.db.close()

    def listening_for(self, message):
        if message['text'] is not None:
            try:
                first_word = message['text'].split()[0]
                if (first_word == '!grab') or (first_word == '!rq') :
                    return True
                else:
                    try:
                        self._log_temp_message(message)
                    except Exception as e:
                        logger.error('Qdb failed to log the temp message.')
                    try:
                        self._cleanup_temp()
                    except Exception as e:
                        logger.error('Qdb failed to cleanup the temp table.')
                    return False
            except Exception as e:
                logger.error('''Qdb lego failed to check message text:
                            {}'''.format(e))
                return False

    def handle(self, message):
        logger.debug('Handling message...')
        opts = self._handle_opts(message)
        # Set a default return_val in case we can't handle our crap
        return_val = '¯\_(ツ)_/¯'
        if message['text'].split()[0] == '!grab':
            last_message_stored = self._store_last_message(message)
            return_val = 'Stored quote: "{}"'.format(last_message_stored)
        elif message['text'].split()[0] == '!rq':
            random_quote = self._get_random_quote()
            return_val = '{} said: "{}" at {}.'.format(random_quote['user'], random_quote['quote'], random_quote['ts'])
        self.reply(message, return_val, opts)

    def _handle_opts(self, message):
        try:
            target = message['metadata']['source_channel']
            opts = {'target': target}
        except IndexError:
            opts = None
            logger.error('''Could not identify message source in message:
                        {}'''.format(str(message)))
        return opts

    def _log_temp_message(self, message):
        if 'user' in message['metadata']:
            user = message['metadata']['user']
        elif 'source_user' in message['metadata']:
            user = message['metadata']['source_user']
        else:
            user = 'NULL'
        try:
            self.db = sqlite3.connect('q.db')
            cursor = self.db.cursor()
            insert_temp = 'INSERT INTO temp(quote, user) VALUES(?,?);'
            cursor.execute(insert_temp, (message['text'], user))
            self.db.commit()
        except Exception as e:
            logger.error(e)
        finally:
            self.db.close()
        return True

    def _get_row_count(self, table):
        try:
            self.db = sqlite3.connect('q.db')
            cursor = self.db.cursor()
            select_count = 'SELECT COUNT(*) FROM {};'.format(table)
            cursor.execute(select_count)
            count = cursor.fetchone()
        except Exception as e:
            logger.error('Error getting count from {}: {}'.format(table, e))
        finally:
            self.db.close()
        return count[0]

    def _cleanup_temp(self):
        count = self._get_row_count('temp')
        if count > 50:
            try:
                self.db = sqlite3.connect('q.db')
                cursor = self.db.cursor()
                difference = count[0] - 50
                select_boundary = '''SELECT id FROM temp ORDER BY id ASC
                                 LIMIT 1 OFFSET {};'''.format(difference)
                cursor.execute(select_boundary)
                id = cursor.fetchone()
                delete_record = 'DELETE FROM temp WHERE id < ?'
                cursor.execute(delete_record, (id[0],))
                logger.info('Deleting excess temp records.')
                self.db.commit()
            except Exception as e:
                logger.error(e)
            finally:
                self.db.close()
        return True

    def _get_last_message(self, message):
        try:
            self.db = sqlite3.connect('q.db')
            self.db.row_factory = sqlite3.Row
            cursor = self.db.cursor()
            select_last_message = 'SELECT * FROM temp WHERE ts < CURRENT_TIMESTAMP ORDER BY ts DESC LIMIT 1;'
            cursor.execute(select_last_message)
            last_message = cursor.fetchone()
        except Exception as e:
            logger.error(e)
        finally:
            self.db.close()
        return last_message

    def _store_last_message(self, message):
        last_message = self._get_last_message(message)
        try:
            self.db = sqlite3.connect('q.db')
            cursor = self.db.cursor()
            insert_temp = 'INSERT INTO quotes(quote, user) VALUES(?,?);'
            cursor.execute(insert_temp, (last_message['quote'], last_message['user']))
            self.db.commit()
        except Exception as e:
            logger.error(e)
        finally:
            self.db.close()
        return last_message['quote']

    def _get_random_quote(self):
        count = self._get_row_count('quotes')
        random_row_num = random.randint(1, count)  # nosec
        try:
            self.db = sqlite3.connect('q.db')
            self.db.row_factory = sqlite3.Row
            cursor = self.db.cursor()
            select_random_quote = 'SELECT * FROM quotes LIMIT 1 OFFSET {};'.format(random_row_num)
            cursor.execute(select_random_quote)
            random_quote = cursor.fetchone()
        except Exception as e:
            logger.error('Error retrieving random quote: {}'.format(e))
        finally:
            self.db.close()
        return random_quote

    def get_name(self):
        return 'qdb'

    def get_help(self):
        return '''Save the last message in the quotes db. Usage: !grab'''
