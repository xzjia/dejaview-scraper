import os
import logging
import sqlalchemy as sa


class Database(object):
    def __init__(self):
        self.logger = logging.getLogger('daily_collector.Database')
        self.conn = self.get_db_conn()
        self.logger.info('Connected to  {}'.format(
            os.environ['DATABASE_URL'].split('@').pop()))
        self.label_table = sa.table('label', sa.column(
            'id', sa.Integer), sa.column('name', sa.Text))
        self.event_table = sa.table('event',
                                    sa.column('timestamp', sa.DateTime),
                                    sa.column('title', sa.Text),
                                    sa.column('text', sa.Text),
                                    sa.column('link', sa.Text),
                                    sa.column('label_id', sa.Integer),
                                    sa.column('image_link', sa.Text),
                                    sa.column('media_link', sa.Text)
                                    )

    def get_db_conn(self):
        engine = sa.create_engine(os.environ['DATABASE_URL'], echo=False)
        return engine.connect()

    def get_label_id_from_name(self, name):
        s = sa.sql.select([self.label_table.c.id, self.label_table.c.name]
                          ).where(self.label_table.c.name == name)
        result = self.conn.execute(s)
        if result.rowcount < 1:
            ins = self.label_table.insert().values(name=name)
            self.conn.execute(ins)
        result = self.conn.execute(s)
        return result.fetchone()['id']

    def get_existing_events(self, label_id, target_timestamp, target_title):
        s = sa.sql.select([self.event_table.c.title, self.event_table.c.timestamp, self.event_table.c.link, self.event_table.c.text, self.event_table.c.image_link, self.event_table.c.media_link]).where(
            sa.and_(
                self.event_table.c.label_id == label_id,
                self.event_table.c.timestamp == target_timestamp,
                self.event_table.c.title == target_title
            )
        )
        result = self.conn.execute(s)
        return result

    def store_rds(self, event_rows, label_id, already_same):
        already_same_count = 0
        update_count = 0
        inserts = []
        for row in event_rows:
            existing_event = self.get_existing_events(
                label_id, row['timestamp'], row['title']).fetchone()
            if existing_event and len(existing_event) > 0:
                if not already_same(existing_event, row):
                    update = self.event_table.update().values(text=row['text'], media_link=row['media_link'], link=row['link'], image_link=row['image_link']).where(sa.and_(
                        self.event_table.c.label_id == label_id,
                        self.event_table.c.timestamp == row['timestamp'],
                        self.event_table.c.title == row['title']
                    ))
                    update_count += 1
                    self.conn.execute(update)
                else:
                    already_same_count += 1
            else:
                inserts.append(row)
        if len(inserts) > 0:
            ins = self.event_table.insert().values(inserts)
            self.conn.execute(ins)
        return len(inserts), update_count, already_same_count
