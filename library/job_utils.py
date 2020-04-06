import datetime
import os
import sys


class Job:

    def __init__(self):
        self.script_name = str(os.path.basename(sys.argv[0]))
        self.start_time = datetime.datetime.now()
        self.status = None
        self._id = str(hash(self.script_name + self.start_time.strftime('%Y%m%d%H%M%S')))

    def set_status(self, status, db=None):
        self.status = status
        if db:
            values = [self._id, self.script_name, self.start_time.strftime('%Y%m%d%H%M%S'), self.status]
            db.insert_row(self, 'Jobs', values)
