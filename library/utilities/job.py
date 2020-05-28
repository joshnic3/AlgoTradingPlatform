import datetime

from library.bootstrap import Constants
from library.interfaces.sql_database import Database, query_result_to_dict
from library.bootstrap import log_hr


def get_run_count(db, script_name, version=None):
    version_index = 3

    condition = 'script="{0}"'.format(script_name.lower())
    results = db.query_table('jobs', condition)
    if version:
        versions = set([r[version_index] for r in results])
        version_to_count = max(versions) if version.lower() == 'latest' else version
        return len([r for r in results if r[version_index] == version_to_count])
    else:
        return len([r for r in results])


def is_script_new(script_name):
    db = Database()
    new_threshold = 50
    no_of_runs_on_latest_version = get_run_count(db, script_name, 'latest')
    if no_of_runs_on_latest_version < new_threshold:
        return True
    return False


class Job:

    SUCCESSFUL = 0
    WARNINGS = 2
    FAILED = 1
    STATUS_MAP = {
        SUCCESSFUL: 'finished successfully',
        WARNINGS: 'finished with warnings',
        FAILED: 'failed'
    }
    FIRST_PHASE = 'INITIATED'

    def __init__(self, log_path=None, job_id=None):
        self._db = Database()
        self.phase_name = None

        if job_id:
            # Load in an existing job from database.
            job_row = self._db.get_one_row('jobs', 'id="{0}"'.format(job_id))
            job_dict = query_result_to_dict([job_row], Constants.configs['tables'][Constants.db_name]['jobs'])[0]

            # Read in job phase.
            phase_row = self._db.query_table('phases', 'job_id="{0}"'.format(job_dict['id']))
            phase_dict = query_result_to_dict(phase_row, Constants.configs['tables'][Constants.db_name]['phases'])[-1]
            job_dict['phase_name'] = phase_dict['name']

        else:
            # Create new job and add it to the database.
            job_dict = self._create_job_dict(log_path)
            self._db.insert_row_from_dict('jobs', job_dict)

        # Set instance variables.
        self.id = job_dict['id']
        self.name = job_dict['name']
        self.script = job_dict['script']
        self.version = job_dict['version']
        self.log_path = job_dict['log_path']
        self.elapsed_time = job_dict['elapsed_time']
        self.finish_state = job_dict['finish_state']
        self.start_time = job_dict['start_time']
        self.phase_name = job_dict['phase_name']

        # Initiate the job is no phase.
        if self.phase_name is None:
            self.update_phase(Job.FIRST_PHASE)

    @staticmethod
    def _create_job_dict(log_path):
        if Constants.job_name:
            name = Constants.job_name
        else:
            name = '{0}_manual_run'.format(Constants.script)
        return {
            'id': str(abs(hash(name + datetime.datetime.now().strftime(Constants.DATETIME_FORMAT)))),
            'name': name.lower(),
            'script': Constants.script,
            'version': Constants.configs['version'],
            'log_path': log_path,
            'elapsed_time': None,
            'finish_state': None,
            'start_time': datetime.datetime.now().strftime(Constants.DATETIME_FORMAT),
            'phase_name': None
        }

    def _add_phase(self, name):
        phase_id = str(abs(hash(name + self.id)))
        date_time = datetime.datetime.now().strftime(Constants.DATETIME_FORMAT)
        self._db.insert_row('phases', [phase_id, self.id, date_time, name])
        return phase_id

    def log(self, logger=None):
        if logger is None:
            logger = Constants.log
        logger.info('Starting job: {0}'.format(self.id))
        log_hr()

    def update_phase(self, phase):
        self.phase_name = phase.replace(' ', '_').upper()
        phase_id = self._add_phase(self.phase_name)
        self._db.update_value('job', 'phase_id', phase_id, 'id="{0}"'.format(self.id))

    def finished(self, status=SUCCESSFUL, condition=None):
        log_hr()

        # Update job phase.
        if condition is None:
            self.update_phase('TERMINATED_SUCCESSFULLY')
        else:
            Constants.log.warning('Job finished early with condition: "{0}"'.format(condition))
            self.update_phase('TERMINATED_{0}'.format(condition))

        # Update job.
        start_time = self._db.get_one_row('phases', 'job_id="{}" AND name="{}"'.format(self.id, Job.FIRST_PHASE))[2]
        start_time = datetime.datetime.strptime(start_time, Constants.DATETIME_FORMAT)
        run_time = round((datetime.datetime.now() - start_time).total_seconds(), 3)
        self._db.update_value('jobs', 'elapsed_time', run_time, 'id="{0}"'.format(self.id))
        self._db.update_value('jobs', 'finish_state', status, 'id="{0}"'.format(self.id))

        # Log final status.
        if status == Job.SUCCESSFUL or status == Job.WARNINGS:
            Constants.log.info('Job "{0}" {1} in {2} seconds.'.format(self.name, Job.STATUS_MAP[status], run_time))
        elif status == Job.FAILED:
            Constants.log.error('Job "{0}" {1} after {2} seconds.'.format(self.name, Job.STATUS_MAP[status], run_time))
        else:
            Constants.log.info('Job "{0}" finished in {1} seconds.'.format(self.name, run_time))
