import functools
import unittest
import uuid
import httplib2
import mock
import stubout
from yagi.handler import http_connection
import yagi
from yagi.handler.cuf_pub_handler import CufPub


class MockResponse(object):
    def __init__(self, status_code=200):
        self.status = status_code


class MockMessage(object):
    def __init__(self, payload):
        self.payload = payload
        self.acknowledged = False

    def ack(self):
        self.acknowledged = True


class CufPubTests(unittest.TestCase):
    """Tests to ensure the ATOM CUF Pub code holds together as expected"""

    def setUp(self):
        self.stubs = stubout.StubOutForTesting()
        config_dict = {
            'atompub': {
                'url': 'http://127.0.0.1:9000/test/test_feed',
                'user': 'user',
                'key': 'key',
                'interval': 30,
                'max_wait': 600,
                'retries': 1,
                'failures_before_reauth': 5,
                'timeout': '120',
            },
            'event_feed': {
                'feed_title': 'feed_title',
                'feed_host': 'feed_host',
                'use_https': False,
                'port': 'port',
                'atom_categories': "DATACENTER=ORD1, REGION=PREPROD-ORD"

            },
            'handler_auth': {
                'method': 'no_auth'
            },
            'cufpub': {
                'url': 'http://127.0.0.1:9000/test/test_feed',
                'user': 'user',
                'key': 'key',
                'interval': 30,
                'max_wait': 600,
                'retries': 1,
                'failures_before_reauth': 5,
                'timeout': '120',

            },
            'nova': {
                'nova_flavor_field_name': 'dummy_flavor_field_name'
            }
        }

        self.handler = CufPub()

        def get(*args, **kwargs):
            val = None
            for arg in args:
                if val:
                    val = val.get(arg)
                else:
                    val = config_dict.get(arg)
                    if not val:
                        return None or kwargs.get('default')
            return val

        def config_with(*args):
            return functools.partial(get, args)

        self.stubs.Set(yagi.config, 'config_with', config_with)
        self.stubs.Set(yagi.config, 'get', get)

    def tearDown(self):
        self.stubs.UnsetAll()

    @mock.patch('httplib2.Http.request', return_value=(MockResponse(201),
        """<atom:entry xmlns:atom="http://www.w3.org/2005/Atom"><atom:id>"""
        """urn:uuid:95347e4d-4737-4438-b774-6a9219d78d2a</atom:id>"""))
    def test_notify_for_instance_exists_message(self, mock_request):
        original_message_id = '425b23c9-9add-409f-938e-c131f304602a'
        messages = [MockMessage(
            {
                'event_type': 'compute.instance.exists',
                'message_id': 'some_uuid',
                'original_message_id': original_message_id,
                'payload':
                    {'tenant_id': '2882',
                     'access_ip_v4': '5.79.20.138',
                     'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                     'audit_period_beginning': '2012-09-15 11:51:11',
                     'audit_period_ending': '2012-09-16 11:51:11',
                     'display_name': 'test',
                     'bandwidth': {'private': {'bw_in': '0', 'bw_out': '264902'},
                                   'public': {'bw_in': '1001', 'bw_out': '19992'}
                                  },
                     'image_meta': {'com.rackspace__1__options': '1'},

                     'instance_id': '56',
                     'dummy_flavor_field_name': '10',
                     'launched_at': '2012-09-15 12:51:11',
                     'deleted_at': '',
                     'instance_type': 'm1.nano',
                     'state': 'active',
                     'state_description': ''}
            }
        )]

        cuf_xml_body = ("""<?xml version="1.0" encoding="utf-8"?>\n"""
        """<atom:entry xmlns:atom="http://www.w3.org"""
        """/2005/Atom"><atom:category term="compute.instance."""
        """exists.verified.cuf"></atom:category><atom:category term"""
        """="original_message_id:425b23c9-9add-409f-938e-c131f304602a">"""
        """</atom:category><atom:title type="text">Server</atom:title>"""
        """<atom:content type="application/xml"><event xmlns="http://"""
        """docs.rackspace.com/core/event" xmlns:nova="http://docs"""
        """.rackspace.com/event/nova" version="1" """
        """id="10c3e7b3-2ac3-5c93-bc4f-ae32e61d9190" resourceId="56" """
        """resourceName="test" dataCenter="ORD1" region="PREPROD-ORD" """
        """tenantId="2882" startTime="2012-09-15T12:51:11Z" """
        """endTime="2012-09-16T11:51:11Z" type="USAGE"><nova:product """
        """version="1" serviceCode="CloudServersOpenStack" resourceType"""
        """="SERVER" flavorId="10" flavorName="m1.nano" status="ACTIVE" """
        """osLicenseType="RHEL" bandwidthIn="1001" bandwidthOut"""
        """="19992"/></event></atom:content></atom:entry>""")

        self.handler.handle_messages(messages, dict())

        self.assertTrue(mock_request.called)
        mock_request.assert_called_with('http://127.0.0.1:9000/test/test_feed',
                                        'POST', body=cuf_xml_body,
                                         headers={'Content-Type': 'application/atom+xml'})

    @mock.patch('httplib2.Http.request', return_value=(MockResponse(201),
        """<atom:entry xmlns:atom="http://www.w3.org/2005/Atom"><atom:id>"""
        """urn:uuid:95347e4d-4737-4438-b774-6a9219d78d2a</atom:id>"""))
    def test_notify_for_image_exists_message_for_one_image(self, mock_request):
        original_message_id = '425b23c9-9add-409f-938e-c131f304602a'
        messages = [MockMessage(
            {"event_type": "image.exists",
            "timestamp": "2013-09-02 16:09:16.247932",
            "message_id": "18b59543-2e99-4208-ba53-22726c02bd67",
            "original_message_id": original_message_id,
            "priority": "INFO",
            "publisher_id": "ubuntu",
            "payload": {
                "owner": "owner1",
                "audit_period_ending": "2013-09-02 23:59:59.999999",
                "audit_period_beginning": "2013-09-02 00:00:00",
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
                }]
        }}
        )]

        cuf_xml_body = ("""<?xml version="1.0" encoding="utf-8"?>\n"""
        """<atom:entry xmlns="http://docs.rackspace."""
        """com/core/event" xmlns:atom="http://www.w3.org/2005/Atom" """
        """xmlns:glance="http://docs.rackspace.com/usage/glance">"""
        """<atom:category term="image.exists.verified.cuf"></atom:category>"""
        """<atom:category term="original_message_id:425b23c9-9add-409f-938e-c131f304602a"></atom:category>"""
        """<atom:title type="text">Glance"""
        """</atom:title><atom:content type="application/xml"><"""
        """event endTime="2013-09-02T23:59:59Z" """
        """startTime="2013-09-02T16:08:10Z" region="PREPROD-ORD" """
        """dataCenter="ORD1" type="USAGE" """
        """id="3ec2aa55-1f5c-59b9-b7c9-f05a8dc5b9e3" resourceId="image1" """
        """tenantId="owner1" version="1"> <glance:product """
        """storage="12345" serverId="inst_uuid1" serviceCode="Glance" """
        """serverName="" resourceType="snapshot" version="1"/></event>"""
        """</atom:content></atom:entry>""")

        self.handler.handle_messages(messages, dict())

        self.assertTrue(mock_request.called)
        mock_request.assert_called_with('http://127.0.0.1:9000/test/test_feed',
                                        'POST', body=cuf_xml_body,
                                         headers={'Content-Type': 'application/atom+xml'})

    @mock.patch('httplib2.Http.request', return_value=(MockResponse(201),
        """<atom:entry xmlns:atom="http://www.w3.org/2005/Atom"><atom:id>"""
        """urn:uuid:95347e4d-4737-4438-b774-6a9219d78d2a</atom:id>"""))
    def test_notify_for_image_exists_message_for_more_than_one_image(self, mock_request):
        original_message_id = '425b23c9-9add-409f-938e-c131f304602a'
        messages = [MockMessage(
            {"event_type": "image.exists",
            "timestamp": "2013-09-02 16:09:16.247932",
            "message_id": "18b59543-2e99-4208-ba53-22726c02bd67",
            "original_message_id": original_message_id,
            "priority": "INFO",
            "publisher_id": "ubuntu",
            "payload": {
                "owner": "owner1",
                "audit_period_ending": "2013-09-02 23:59:59.999999",
                "audit_period_beginning": "2013-09-02 00:00:00",
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
                }]
        }}
        )]

        cuf_xml_body = ("""<?xml version="1.0" encoding="utf-8"?>\n"""
        """<atom:entry xmlns="http://docs.rackspace."""
        """com/core/event" xmlns:atom="http://www.w3.org/2005/Atom" """
        """xmlns:glance="http://docs.rackspace.com/usage/glance">"""
        """<atom:category term="image.exists.verified.cuf"></atom:category>"""
        """<atom:category term="original_message_id:425b23c9-9add-409f-938e-c131f304602a"></atom:category>"""
        """<atom:title type="text">Glance</atom:title><atom:content type="application/xml">"""
        """<event endTime="2013-09-02T23:59:59Z" startTime="2013-09-02T"""
        """16:08:10Z" region="PREPROD-ORD" dataCenter="ORD1" type="USAGE" """
        """id="3ec2aa55-1f5c-59b9-b7c9-f05a8dc5b9e3" resourceId="image1" """
        """tenantId="owner1" version="1"> <glance:product storage="12345" """
        """serverId="inst_uuid1" serviceCode="Glance" serverName="" """
        """resourceType="snapshot" version="1"/></event><event """
        """endTime="2013-09-02T16:08:46Z" startTime="2013-09-02T16:05:17Z" """
        """region="PREPROD-ORD" dataCenter="ORD1" type="USAGE" """
        """id="8809eae4-2e0e-52a6-b95a-a608ee3acb91" resourceId="image2" """
        """tenantId="owner1" version="1"> <glance:product storage="67890" """
        """serverId="inst_uuid2" serviceCode="Glance" serverName="" """
        """resourceType="snapshot" version="1"/></event>"""
        """</atom:content></atom:entry>""")

        self.handler.handle_messages(messages, dict())

        self.assertTrue(mock_request.called)
        mock_request.assert_called_with('http://127.0.0.1:9000/test/test_feed',
                                        'POST', body=cuf_xml_body,
                                         headers={'Content-Type': 'application/atom+xml'})

    @mock.patch('httplib2.Http.request', return_value=(MockResponse(404), """Bogus, dude!"""))
    def test_notify_fails(self, mock_request):
        messages = [MockMessage(
            {
                'event_type': 'compute.instance.exists',
                'message_id': 'some_uuid',
                '_unique_id': 'e53d007a-fc23-11e1-975c-cfa6b29bb814',
                'payload':
                    {'tenant_id': '2882',
                     'access_ip_v4': '5.79.20.138',
                     'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                     'audit_period_beginning': '2012-09-15 11:51:11',
                     'audit_period_ending': '2012-09-16 11:51:11',
                     'bandwidth': {'private': {'bw_in': 0, 'bw_out': 264902},
                                   'public': {'bw_in': 1001, 'bw_out': 19992}
                                  },
                     'image_meta': {'com.rackspace__1__options': '1'},

                     'instance_id': '56',
                     'dummy_flavor_field_name': '40',
                     'launched_at': '2012-09-15 12:51:11',
                     'deleted_at': '',
                     'instance_type': 'm1.nano',
                     'state': 'active',
                     'state_description': ''}
            }
        )]

        self.handler.handle_messages(messages, dict())

        self.assertTrue(mock_request.called)

    @mock.patch('httplib2.Http.request', return_value=(MockResponse(404), """Bogus, dude!"""))
    def test_malformed_message_should_not_raise_exception(self, mock_request):
        messages = [MockMessage(
            {
                'event_type': 'compute.instance.exists',
                'message_id': 'some_uuid',
                '_unique_id': 'e53d007a-fc23-11e1-975c-cfa6b29bb814',
                'payload':
                    {'tenant_id': '2882',
                     'access_ip_v4': '5.79.20.138',
                     'access_ip_v6': '2a00:1a48:7804:0110:a8a0:fa07:ff08:157a',
                     'audit_period_beginning': '2012-09-15 11:51:11',
                     'bandwidth': {'private': {'bw_in': 0, 'bw_out': 264902},
                                   'public': {'bw_in': 1001, 'bw_out': 19992}
                                  },
                     'image_meta': {'com.rackspace__1__options': '1'},

                     'instance_id': '56',
                     'dummy_flavor_field_name': '40',
                     'launched_at': '2012-09-15 12:51:11',
                     'deleted_at': ''}
            }
        )]
        self.handler.handle_messages(messages, dict())

        self.assertFalse(mock_request.called)


    @mock.patch('httplib2.Http.request', return_value=(MockResponse(201),
        """<atom:entry xmlns:atom="http://www.w3.org/2005/Atom"><atom:id>"""
        """urn:uuid:95347e4d-4737-4438-b774-6a9219d78d2a</atom:id>"""))
    def test_notify_for_pub_ipv4_exists_message(self, mock_request):
        """This essentially checks that 'ip.exists' is a valid event_type"""
        original_message_id = '425b23c9-9add-409f-938e-c131f304602a'
        messages = [MockMessage(
            {
                "_unique_id": original_message_id,
                "event_type": "ip.exists",
                "timestamp": "2016-06-13T23:59:59Z",
                "message_id": "18b59543-2e99-4208-ba53-22726c02bd67",
                "priority": "INFO",
                "publisher_id": "ubuntu",
                "payload": {
                    "endTime": "2016-06-13T23:59:59Z",
                    "startTime": "2016-06-13T00:00:00Z",
                    "id": "ffc6f692-b31b-4ea8-8056-fbbaefa86e34",
                    "ip_address": "10.69.221.27",
                    "ip_type": "fixed",
                    "tenant_id": "404"
                }
            }
        )]

        cuf_xml_body = (
            """<?xml version="1.0" encoding="utf-8"?>\n"""
            """<atom:entry xmlns:atom="http://www.w3.org/2005/Atom"><atom:category """
            """term="neutron.ip.exists.verified.cuf"></atom:category><atom:category """
            """term="original_message_id:425b23c9-9add-409f-938e-c131f304602a"></atom:category><atom:title """
            """type="text">NeutronPubIPv4</atom:title><atom:content """
            """type="application/xml"><event """
            """xmlns="http://docs.rackspace.com/core/event" """
            """xmlns:neutron="http://docs.rackspace.com/usage/neutron/public-ip-usage" """
            """id="3ac28cdc-54ed-55ac-9f51-e00d166d4044" version="1" """
            """resourceId="ffc6f692-b31b-4ea8-8056-fbbaefa86e34" """
            """resourceName="10.69.221.27" tenantId="404" """
            """startTime="2016-06-13T00:00:00Z" endTime="2016-06-13T23:59:59Z" """
            """type="USAGE" dataCenter="ORD1" region="PREPROD-ORD"> <neutron:product """
            """serviceCode="CloudNetworks" resourceType="IP" ipType="fixed"/> """
            """</event></atom:content></atom:entry>"""
        )

        self.handler.handle_messages(messages, dict())
        self.assertTrue(mock_request.called)
        mock_request.assert_called_with('http://127.0.0.1:9000/test/test_feed',
                                        'POST', body=cuf_xml_body,
                                         headers={'Content-Type': 'application/atom+xml'})
