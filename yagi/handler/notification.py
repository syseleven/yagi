import os
import uuid
import datetime
import logging
from yagi.handler.notification_options import NotificationOptions
from yagi.handler.notification_payload import NotificationPayload,\
        GlanceNotificationPayload,\
        NeutronPubIPv4UsageNotificationPayload

nova_cuf_template = ("""<event xmlns="http://docs.rackspace.com/core/event" """
"""xmlns:nova="http://docs.rackspace.com/event/nova" version="1" """
"""id="%(id)s" resourceId="%(instance_id)s" """
"""resourceName="%(instance_name)s" dataCenter="%(data_center)s" """
"""region="%(region)s" tenantId="%(tenant_id)s" startTime="%(start_time)s" """
"""endTime="%(end_time)s" type="USAGE"><nova:product version="1" """
"""serviceCode="CloudServersOpenStack" resourceType="SERVER" flavorId"""
"""="%(flavor_id)s" flavorName="%(flavor_name)s" status="%(status)s" """
"""%(options_string)s bandwidthIn="%(bandwidth_in)s" """
"""bandwidthOut="%(bandwidth_out)s"/></event>""")

glance_cuf_template_per_image = ("""<event endTime="%(end_time)s" """
"""startTime="%(start_time)s" region="%(region)s" """
"""dataCenter="%(data_center)s" type="USAGE" id="%(id)s" """
"""resourceId="%(resource_id)s" tenantId="%(tenant_id)s" version="1"> """
"""<glance:product storage="%(storage)s" serverId="%(server_id)s" """
"""serviceCode="Glance" serverName="%(server_name)s" """
"""resourceType="%(resource_type)s" version="1"/></event>""")

neutron_pub_ipv4_v1_cuf_template = (
"""<event xmlns="http://docs.rackspace.com/core/event" """
"""xmlns:neutron="http://docs.rackspace.com/usage/neutron/ipuptime" """
"""id="{id}" """
"""version="1" """
"""resourceId="{resourceId}" """
"""resourceName="{resourceName}" """
"""tenantId="{tenantId}" """
"""startTime="{startTime}" """
"""endTime="{endTime}" """
"""type="USAGE" """
"""dataCenter="{dataCenter}" """
"""region="{region}"> """
"""<neutron:product serviceCode="CloudNetworks" """
"""resourceType="{resourceType}" """
"""version="1" """
"""ipType="{ipType}"/> """
"""</event>"""
)

atom_hopper_time_format = "%Y-%m-%dT%H:%M:%SZ"

LOG = logging.getLogger(__name__)


class BaseNotification(object):

    def __init__(self, message, event_type, region="", data_center=""):
        self.message = message
        self.event_type = event_type
        self.region = region
        self.data_center = data_center

    def get_original_message_id(self):
        return self.message.get('original_message_id', "")

    def generate_new_id(self, extra=None):
        # Generate message_id for new events deterministically from
        # the original message_id and event type using uuid5 algo.
        # This will allow any dups to be caught by message_id. (mdragon)
        original_message_id = self.get_original_message_id()

        if original_message_id:
            oid = uuid.UUID(original_message_id)
            if extra:
                return uuid.uuid5(oid, self.event_type + str(extra))
            else:
                return uuid.uuid5(oid, self.event_type)
        else:
            LOG.error("Generating %s, but origional message missing"
                      " origional_message_id." % self.event_type)
            return uuid.uuid4()

    def convert_to_verified_message_in_cuf_format(self):
        cuf_xml = self._create_cuf_xml(self.message)
        return {'payload': cuf_xml}

    def to_entity(self):
        cuf = self.convert_to_verified_message_in_cuf_format()
        entity = dict(content=cuf,
                      id=str(self.generate_new_id()),
                      event_type=self.event_type,
                      original_message_id=self.get_original_message_id())
        return entity


class Notification(BaseNotification):

    def _create_cuf_xml(self, json_body):
        payload = NotificationPayload(json_body['payload'])
        notification_options = {'com.rackspace__1__options': payload.options}
        cuf_xml_values = {}
        cuf_xml_values['options_string'] = NotificationOptions(
            notification_options).to_cuf_options()
        cuf_xml_values['bandwidth_in'] = payload.bandwidth_in
        cuf_xml_values['bandwidth_out'] = payload.bandwidth_out
        cuf_xml_values['start_time'] = datetime.datetime.strftime(
            payload.start_time, atom_hopper_time_format)
        cuf_xml_values['end_time'] = datetime.datetime.strftime(
            payload.end_time, atom_hopper_time_format)
        cuf_xml_values['tenant_id'] = payload.tenant_id
        cuf_xml_values['instance_id'] = payload.instance_id
        cuf_xml_values['instance_name'] = payload.instance_name
        cuf_xml_values['id'] = self.generate_new_id()
        cuf_xml_values['flavor_id'] = payload.flavor_id
        cuf_xml_values['flavor_name'] = payload.flavor_name
        cuf_xml_values['status'] = payload.status
        cuf_xml_values['data_center'] = self.data_center
        cuf_xml_values['region'] = self.region
        cuf_xml = nova_cuf_template % cuf_xml_values
        return cuf_xml


class GlanceNotification(BaseNotification):

    def _create_cuf_xml(self, json_body):
        payload = GlanceNotificationPayload(json_body['payload'], atom_hopper_time_format)
        images = payload.images
        cuf = "<events>"
        for image in images:
            image['id'] = self.generate_new_id(extra=image['resource_id'])
            image['data_center'] = self.data_center
            image['region'] = self.region
            cuf_xml = glance_cuf_template_per_image % image
            cuf += cuf_xml
        cuf += "</events>"
        return cuf


class NeutronPubIPv4UsageNotification(BaseNotification):
    """Neutron Base Notification

    This will build the base for Neutron additional IP notifications and
    Floating IP bandwidth notifications.
    """

    def get_original_message_id(self):
        return self.message.get('_unique_id', "")


    def _create_cuf_xml(self, json_body):
        payload = NeutronPubIPv4UsageNotificationPayload(json_body['payload'])
        cuf_xml_values = {
            'id': str(self.generate_new_id()),
            'resourceId': payload.resourceId,
            'resourceName': payload.resourceName,
            'tenantId': payload.tenantId,
            'startTime': payload.startTime,
            'endTime': payload.endTime,
            'resourceType': payload.resourceType,
            'ipType': payload.ipType,
            'dataCenter': self.data_center,
            'region': self.region
        }

        cuf_xml = neutron_pub_ipv4_v1_cuf_template.format(**cuf_xml_values)
        return cuf_xml
