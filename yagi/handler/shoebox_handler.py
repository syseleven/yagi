import json
import logging
import os

import notification_utils
from shoebox import roll_manager
import simport

import yagi.config
import yagi.handler
import yagi.utils

LOG = logging.getLogger(__name__)


class ShoeboxHandler(yagi.handler.BaseHandler):
    """The shoebox handler doesn't really fit with
       yagi persistence interface currently. We'll need
       to refactor one or the other to clean it up.
    """

    CONFIG_SECTION = "shoebox"

    def __init__(self, app=None, queue_name=None):
        super(ShoeboxHandler, self).__init__(app, queue_name)
        # Don't use interpolation from ConfigParser ...
        self.config = self.config_getsection(raw=True)
        roll_checker_str = self.config.pop('roll_checker', None)
        self.roll_checker = None
        if roll_checker_str:
            self.roll_checker = simport.load(roll_checker_str)(**self.config)
        self.working_directory = self.config.pop('working_directory', '.')
        self.destination_folder = self.config.pop('destination_folder', '.')
        for d in [self.working_directory, self.destination_folder]:
            if not os.path.isdir(d):
                os.makedirs(d)
        template=self.config.pop('filename_template',
                                 'events_%Y_%m_%d_%X_%f.dat')
        callback_str = self.config.pop('callback', None)
        cb = None
        if callback_str:
            cb = simport.load(callback_str)(**self.config)

        roll_manager_str = self.config.pop('roll_manager',
                                    'shoebox.roll_manager:WritingRollManager')

        self.wrap_payload_with_region = self.config.pop(
                            'wrap_payload_with_region', 'False') == 'True'

        self.region = self.config.pop('wrap_region', 'n/a')
        self.cell = self.config.pop('wrap_cell', 'n/a')

        self.roll_manager = simport.load(roll_manager_str)(template,
                        self.roll_checker, directory=self.working_directory,
                        destination_directory=self.destination_folder,
                        archive_callback=cb, **self.config)

    def handle_messages(self, messages, env):
        # TODO(sandy): iterate_payloads filters messages first ... not
        # sure if we want that for raw archiving.
        for payload in self.iterate_payloads(messages, env):
            metadata = {}
            if self.wrap_payload_with_region:
                payload = {'region': self.region, 'cell': self.cell,
                           'notification': payload}
            json_event = json.dumps(payload,
                                    cls=notification_utils.DateTimeEncoder)
            LOG.debug("shoebox writing payload: %s" % str(payload))
            self.roll_manager.write(metadata, json_event)
