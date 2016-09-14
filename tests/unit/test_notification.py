from unittest import TestCase
import uuid
import datetime
import mox
from tests.unit import utils
from yagi.handler import notification_payload
from yagi.handler.cuf_pub_handler import Notification
from yagi.handler.notification import GlanceNotification
from yagi.handler.notification_payload import NotificationPayload
import yagi.config


class TestGlanceNotification(TestCase):

    def setUp(self):
        self.mox = mox.Mox()

    def tearDown(self):
        self.mox.UnsetStubs()

    def test_convert_to_cuf_format(self):
        self.maxDiff = None
        event_type = 'image.exists.verified.cuf'
        exists_message = {
            "event_type": "image.exists",
            "timestamp": "2013-09-02 16:09:16.247932",
            "message_id": "18b59543-2e99-4208-ba53-22726c02bd67",
            "priority": "INFO",
            "publisher_id": "ubuntu",
            "payload": {
                "images": [{
                    "status": "active",
                    "name": "image1",
                    "created_at": "2013-09-02 16:08:10",
                    "properties": {
                        "image_type": "snapshot",
                        "instance_uuid": "inst_uuid1"
                    },
                    "deleted_at": None,
                    "id": "image1",
                    "size": 12345
                },
                {
                    "status": "deleted",
                    "name": "image2",
                    "created_at": "2013-09-02 16:05:17",
                    "properties": {
                        "image_type": "snapshot",
                        "instance_uuid": "inst_uuid2"
                    },
                    "deleted_at": "2013-09-02 16:08:46",
                    "id": "image2",
                    "size": 67890
                }],
                "owner": "owner1",
                "audit_period_ending": "2013-09-02 23:59:59.999999",
                "audit_period_beginning": "2013-09-02 00:00:00"
            }
        }
        self.mox.StubOutWithMock(uuid, 'uuid4')
        uuid.uuid4().AndReturn('uuid1')
        uuid.uuid4().AndReturn('uuid2')
        self.mox.StubOutWithMock(notification_payload, 'start_time')
        self.mox.StubOutWithMock(notification_payload, 'end_time')
        notification_payload.start_time(
            '2013-09-02 16:08:10',
            '2013-09-02 00:00:00').AndReturn(datetime.datetime(2013,9,2))
        notification_payload.end_time(
            None,
            '2013-09-02 23:59:59.999999').AndReturn(datetime.datetime(
                2013, 9, 2, 23, 59, 59, 999999))
        notification_payload.start_time(
            '2013-09-02 16:05:17',
            '2013-09-02 00:00:00').AndReturn(datetime.datetime(
                2013, 9, 2, 16, 5, 17))
        notification_payload.end_time(
            '2013-09-02 16:08:46',
            '2013-09-02 23:59:59.999999').AndReturn(datetime.datetime(
                2013, 9, 2, 23, 59, 59, 999999))
        self.mox.ReplayAll()
        expected_cuf_xml = {'payload': ("""<event endTime="2013-09-02T23:59:59Z" """
        """startTime="2013-09-02T00:00:00Z" region="DFW" dataCenter="DFW1" """
        """type="USAGE" id="uuid1" resourceId="image1" tenantId="owner1" """
        """version="1"> <glance:product storage="12345" serverId="inst_uuid1" """
        """serviceCode="Glance" serverName="" resourceType="snapshot" """
        """version="1"/></event><event endTime="2013-09-02T23:59:59Z" """
        """startTime="2013-09-02T16:05:17Z" region="DFW" dataCenter="DFW1" """
        """type="USAGE" id="uuid2" resourceId="image2" tenantId="owner1" """
        """version="1"> <glance:product storage="67890" serverId="inst_uuid2" """
        """serviceCode="Glance" serverName="" resourceType="snapshot" """
        """version="1"/></event>""")}
        notification = GlanceNotification(exists_message,
                                          event_type=event_type,
                                          region='DFW',
                                          data_center='DFW1')
        verified_message = notification.\
            convert_to_verified_message_in_cuf_format()

        self.assertEquals(verified_message, expected_cuf_xml)
        self.mox.VerifyAll()

    def test_start_time_should_return_launched_at_when_launched_at_is_bigger(self):
        start_time = notification_payload.start_time('2013-09-02 16:08:10',
            '2013-09-02 00:00:00')
        self.assertEqual(str(start_time), "2013-09-02 16:08:10")

    def test_end_time_should_return_deleted_at_when_deleted_at_is_smaller(self):
        end_time = notification_payload.end_time(
            '2013-09-02 16:08:46',
            '2013-09-02 23:59:59.999999')
        self.assertEqual(str(end_time), "2013-09-02 16:08:46")

    def test_end_time_should_return_audit_period_beginning_when_deleted_at_is_none(self):
        end_time = notification_payload.end_time(
            "",
            '2013-09-02 23:59:59.999999')
        self.assertEqual(str(end_time), "2013-09-02 23:59:59.999999")

class TestNovaNotification(TestCase):

    def setUp(self):
        self.event_type = 'compute.instance.exists.verified.cuf'
        self.region = "DFW"
        self.data_center = "DFW1"
        self.mox = mox.Mox()
        self.mox.StubOutWithMock(yagi.config, 'get')
        yagi.config.get('nova', 'nova_flavor_field_name').AndReturn(
            'dummy_flavor_field_name')
        self.mox.ReplayAll()

    def tearDown(self):
        self.mox.UnsetStubs()

    def test_convert_to_cuf_deterministic_id(self):
        det_id = '00efc101-1a92-528e-bd71-7fa023d4e952'
        exists_message = {
            'event_type': 'test',
            'message_id': 'some_uuid',
            'original_message_id': '7f2f0e12-fa8a-49ac-985e-d74d06a38750',
            '_unique_id': 'e53d007a-fc23-11e1-975c-cfa6b29bb814',
            'payload': {
                'tenant_id': '2882',
                'access_ip_v4': '5.79.20.138',
                'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                'audit_period_beginning': '2012-09-15 11:51:11',
                'audit_period_ending': '2012-09-16 11:51:11',
                'display_name': 'test',
                'bandwidth': {
                    'private': {'bw_in': 0, 'bw_out': 264902},
                    'public': {'bw_in': 1001,'bw_out': 19992}
                },
                'image_meta': {'com.rackspace__1__options': '1'},
                'instance_id': '56',
                'dummy_flavor_field_name': '10',
                'launched_at': '2012-09-14 11:51:11',
                'deleted_at': '',
                'instance_type': 'm1.nano',
                'state': 'active',
                'state_description': ''
            }
        }
        self.mox.StubOutWithMock(uuid, 'uuid4')
        uuid.uuid4().AndReturn('e53d007a-fc23-11e1-975c-cfa6b29bb814')
        self.mox.ReplayAll()
        changed_values = dict(id=det_id)
        expected_cuf_xml = utils.verified_nova_message_in_cuf_format(
            changed_values)

        notification = Notification(exists_message,
                                    event_type=self.event_type,
                                    region=self.region,
                                    data_center=self.data_center)
        verified_message = notification.\
            convert_to_verified_message_in_cuf_format()

        self.assertEquals(expected_cuf_xml['payload'],
                          verified_message['payload'])

    def test_convert_to_cuf_format_when_launched_at_before_audit_period(self):
        exists_message = {
            'event_type': 'test',
            'message_id': 'some_uuid',
            '_unique_id': 'e53d007a-fc23-11e1-975c-cfa6b29bb814',
            'payload': {
                'tenant_id': '2882',
                'access_ip_v4': '5.79.20.138',
                'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                'audit_period_beginning': '2012-09-15 11:51:11',
                'audit_period_ending': '2012-09-16 11:51:11',
                'display_name': 'test',
                'bandwidth': {
                    'private': {'bw_in': 0, 'bw_out': 264902},
                    'public': {'bw_in': 1001,'bw_out': 19992}
                },
                'image_meta': {'com.rackspace__1__options': '1'},
                'instance_id': '56',
                'dummy_flavor_field_name': '10',
                'launched_at': '2012-09-14 11:51:11',
                'deleted_at': '',
                'instance_type': 'm1.nano',
                'state': 'active',
                'state_description': ''
            }
        }
        self.mox.StubOutWithMock(uuid, 'uuid4')
        uuid.uuid4().AndReturn('e53d007a-fc23-11e1-975c-cfa6b29bb814')
        self.mox.ReplayAll()
        values = utils.test_nova_xml_generator_values
        values.update({'end_time': '2012-09-16T11:51:11Z'})
        expected_cuf_xml = utils.verified_nova_message_in_cuf_format(
            values)

        notification = Notification(exists_message,
                                    event_type=self.event_type,
                                    region=self.region,
                                    data_center=self.data_center)
        verified_message = notification.\
            convert_to_verified_message_in_cuf_format()

        self.assertEquals(expected_cuf_xml['payload'],
                          verified_message['payload'])

    def test_convert_to_cuf_format_when_launched_at_in_audit_period(self):
        audit_period_beginning = '2012-09-15 10:51:11'
        audit_period_ending = '2012-09-16 10:51:11'
        launched_at = '2012-09-15 11:51:11'
        exists_message = {
            'event_type': 'test',
            'message_id': 'some_uuid',
            '_unique_id': 'e53d007a-fc23-11e1-975c-cfa6b29bb814',
            'payload': {
                'tenant_id': '2882',
                'access_ip_v4': '5.79.20.138',
                'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                'audit_period_beginning': audit_period_beginning,
                'audit_period_ending': audit_period_ending,
                'bandwidth': {'private': {'bw_in': 0, 'bw_out': 264902},
                              'public': {'bw_in': 1001,'bw_out': 19992}},
                'display_name': 'test',
                'image_meta': {'com.rackspace__1__options': '1'},
                'instance_id': '56',
                'dummy_flavor_field_name': '10',
                'launched_at': launched_at,
                'deleted_at': '',
                'instance_type': 'm1.nano',
                'state': 'active',
                'state_description': ''
            }
        }
        self.mox.StubOutWithMock(uuid, 'uuid4')
        uuid.uuid4().AndReturn('e53d007a-fc23-11e1-975c-cfa6b29bb814')
        self.mox.ReplayAll()
        values = utils.test_nova_xml_generator_values
        values.update({'end_time': '2012-09-16T10:51:11Z'})
        expected_cuf_xml = utils.verified_nova_message_in_cuf_format(
            values)
        notification = Notification(exists_message,
                                    event_type=self.event_type,
                                    region=self.region,
                                    data_center=self.data_center)
        verified_message = notification.\
            convert_to_verified_message_in_cuf_format()

        self.assertEquals(expected_cuf_xml['payload'],
                          verified_message['payload'])

    def test_convert_to_cuf_format_when_deleted_at_in_audit_period(self):
        audit_period_beginning = '2012-09-15 10:51:11'
        audit_period_ending = '2012-09-16 10:51:11'
        launched_at = '2012-09-15 11:51:11'
        deleted_at = '2012-09-15 09:51:11'
        exists_message = {
            'event_type': 'test',
            'message_id': 'some_uuid',
            '_unique_id': 'e53d007a-fc23-11e1-975c-cfa6b29bb814',
            'payload': {
                'tenant_id':'2882',
                'access_ip_v4': '5.79.20.138',
                'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                'audit_period_beginning': audit_period_beginning,
                'audit_period_ending': audit_period_ending,
                'display_name': 'test',
                'bandwidth': {'private': {'bw_in': 0, 'bw_out': 264902},
                              'public': {'bw_in': 1001,'bw_out': 19992}},
                'image_meta': {'com.rackspace__1__options': '1'},
                'instance_id': '56',
                'dummy_flavor_field_name': '10',
                'launched_at': launched_at,
                'deleted_at': deleted_at,
                'instance_type': 'm1.nano',
                'state': 'active',
                'state_description': ''
            }
        }
        values = utils.test_nova_xml_generator_values
        values.update({'end_time': '2012-09-15T09:51:11Z'})
        self.mox.StubOutWithMock(uuid, 'uuid4')
        uuid.uuid4().AndReturn('e53d007a-fc23-11e1-975c-cfa6b29bb814')
        expected_cuf_xml = utils.verified_nova_message_in_cuf_format(
            values)
        notification = Notification(exists_message,
                                    event_type=self.event_type,
                                    region=self.region,
                                    data_center=self.data_center)
        self.mox.ReplayAll()
        verified_message = notification.\
            convert_to_verified_message_in_cuf_format()

        self.assertEquals(expected_cuf_xml['payload'],
                          verified_message['payload'])


class TestNotificationPayload(TestCase):

    def setUp(self):
        self.mox = mox.Mox()
        self.mox.StubOutWithMock(yagi.config, 'get')
        yagi.config.get('nova', 'nova_flavor_field_name').AndReturn(
            'dummy_flavor_field_name')
        self.mox.ReplayAll()

    def tearDown(self):
        self.mox.UnsetStubs()

    def test_start_time_should_get_max_of_launched_at_and_audit_period_beginning(self):
        payload_json ={'tenant_id':'2882',
                     'access_ip_v4': '5.79.20.138',
                     'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                     'audit_period_beginning': '2012-09-15 10:51:11',
                     'audit_period_ending': '2012-09-16 10:51:11',
                     'bandwidth': {'private': {'bw_in': 0, 'bw_out': 264902},
                                   'public': {'bw_in': 1001,'bw_out': 19992}
                                  },
                     'image_meta': {'com.rackspace__1__options': '1'},
                     'instance_id': '56',
                     'dummy_flavor_field_name': '10',
                     'launched_at': '2012-09-15 11:51:11',
                     'deleted_at': '2012-09-15 09:51:11',
                     'instance_type': 'm1.nano',
                     'state': 'active',
                     'state_description': ''
                    }
        payload = NotificationPayload(payload_json)
        self.assertEquals(payload.start_time,
                          datetime.datetime(2012, 9, 15, 11, 51, 11))

    def test_end_time_should_get_min_of_deleted_at_and_audit_period_ending(self):
        payload_json ={'tenant_id':'2882',
                     'access_ip_v4': '5.79.20.138',
                     'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                     'audit_period_beginning': '2012-09-15 10:51:11',
                     'audit_period_ending': '2012-09-16 10:51:11',
                     'bandwidth': {'private': {'bw_in': 0, 'bw_out': 264902},
                                   'public': {'bw_in': 1001,'bw_out': 19992}
                                  },
                     'image_meta': {'com.rackspace__1__options': '1'},
                     'instance_id': '56',
                     'dummy_flavor_field_name': '10',
                     'launched_at': '2012-09-15 11:51:11',
                     'deleted_at': '2012-09-15 09:51:11',
                     'instance_type': 'm1.nano',
                     'state': 'active',
                     'state_description': ''
                    }
        payload = NotificationPayload(payload_json)
        self.assertEquals(str(payload.end_time), '2012-09-15 09:51:11')

    def test_end_time_should_be_audit_period_ending_when_deleted_at_is_empty(self):
        payload_json ={'tenant_id':'2882',
                     'access_ip_v4': '5.79.20.138',
                     'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                     'audit_period_beginning': '2012-09-15 10:51:11',
                     'audit_period_ending': '2012-09-16 10:51:11',
                     'bandwidth': {'private': {'bw_in': 0, 'bw_out': 264902},
                                   'public': {'bw_in': 1001,'bw_out': 19992}
                                  },
                     'image_meta': {'com.rackspace__1__options': '1'},
                     'instance_id': '56',
                     'dummy_flavor_field_name': '10',
                     'launched_at': '2012-09-15 11:51:11',
                     'deleted_at': '',
                     'instance_type': 'm1.nano',
                     'state': 'active',
                     'state_description': ''
                    }
        payload = NotificationPayload(payload_json)
        self.assertEquals(payload.end_time,
                          datetime.datetime(2012, 9, 16, 10, 51, 11))

    def test_different_time_formats_should_not_raise_exception(self):
        payload_json ={'tenant_id':'2882',
                     'access_ip_v4': '5.79.20.138',
                     'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                     'audit_period_beginning': '2012-09-15T10:51:11Z',
                     'audit_period_ending': '2012-09-16 10:51:11',
                     'audit_period_ending': '2012-09-16T10:51:11.799674',
                     'bandwidth': {'private': {'bw_in': 0, 'bw_out': 264902},
                                   'public': {'bw_in': 1001,'bw_out': 19992}
                                  },
                     'image_meta': {'com.rackspace__1__options': '1'},
                     'instance_id': '56',
                     'dummy_flavor_field_name': '10',
                     'launched_at': '2012-09-15 11:51:11',
                     'deleted_at': '',
                     'instance_type': 'm1.nano',
                     'state': 'active',
                     'state_description': ''
                    }
        payload = NotificationPayload(payload_json)
        self.assertEquals(payload.end_time,
                          datetime.datetime(2012, 9, 16, 10, 51, 11, 799674))

