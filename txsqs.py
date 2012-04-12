import base64
import hashlib
import hmac
import urllib
import urlparse
import lxml.etree as etree

from twisted.web import client
from datetime import datetime

class ReceiveMessage(object):
    ''' Returns a function object that can read messages from the given queue. '''

    method = 'GET'
    action = 'ReceiveMessage'
    version = '2011-10-01'

    def __init__(self, queue_url, aws_access_key_id, aws_secret_access_key):
        self.queue_url = str(queue_url)
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        parts = urlparse.urlsplit(queue_url)
        self.host = parts.netloc
        self.path = parts.path

    def __call__(self):
        request_url = self._generate_request_url()
        return client.getPage(request_url) \
                .addCallback(self._extract_messages_from_response) \
                .addCallback(self._return_first_message)

    def _generate_request_url(self):
        query_params = [
            ('Action', self.action),
            ('AWSAccessKeyId', self.aws_access_key_id),
            ('Version', self.version),
            ('SignatureVersion', '2'),
            ('SignatureMethod', 'HmacSHA256'),
            ('Timestamp', datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')),
        ]
        query_params.sort()
        query_params.append(('Signature', self._calculate_signature(query_params)))
        query_string = urlencode_quote(query_params)
        return '%s?%s' % (self.queue_url, query_string)

    def _calculate_signature(self, query_params_list):
        query_string = urlencode_quote(query_params_list)
        string_to_sign = '%s\n%s\n%s\n%s' % (self.method, self.host, self.path, query_string)
        hmac_digest = hmac.new(self.aws_secret_access_key, string_to_sign, hashlib.sha256).digest()
        hmac_base64 = base64.encodestring(hmac_digest).strip()
        return hmac_base64

    @staticmethod
    def _extract_messages_from_response(xml_response):
        ''' Extracts response messages from the aws xml response. '''
        return map(Base64EncodedResponseMessage, xpath(etree.fromstring(xml_response), '//sqs:Message'))

    @staticmethod
    def _return_first_message(message_list):
        return message_list[0] if message_list else None


class RawResponseMessage(object):
    ''' Provides easy access to the data in an aws response message. '''

    def __init__(self, message_xml):
        self._message_xml = message_xml

    def id(self):
        return self._extract_value_for_tag('MessageId')

    def body(self):
        return self._extract_value_for_tag('Body')

    def body_md5sum(self):
        return self._extract_value_for_tag('MD5OfBody')

    def _extract_value_for_tag(self, tag):
        return xpath(self._message_xml, 'sqs:%s' % tag)[0].text


class Base64EncodedResponseMessage(RawResponseMessage):
    ''' A response message which decodes base64 encoded bodies for you. '''

    def body(self):
        return base64.decodestring(super(Base64EncodedResponseMessage, self).body())


def urlencode_quote(query_params_list):
    ''' Like urllib.urlencode, but uses urllib.quote instead of urllib.quote_plus. '''
    quote_plus = urllib.quote_plus
    urllib.quote_plus = urllib.quote
    ret = urllib.urlencode(query_params_list)
    urllib.quote_plus = quote_plus
    return ret

def ns_aware_xpath(**namespaces):
    ''' Returns an xpath function that knows of the given namespaces. '''
    def xpath(ctx, path):
        return ctx.xpath(path, namespaces=namespaces)
    return xpath

xpath = ns_aware_xpath(sqs='http://queue.amazonaws.com/doc/2011-10-01/')
