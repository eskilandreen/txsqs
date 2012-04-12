import os
import txsqs
import boto.sqs
import nose.tools as nt
import nose.twistedtools as tt
from twisted.internet import defer

conn = None
boto_conn = None
boto_queue = None
receive_message = None

def setup(module):
    # Fetch credentials from environment.
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    assert aws_access_key_id and aws_secret_access_key, \
            'You must set environmental variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY'

    # These could be configurable too.
    region_name = 'eu-west-1'
    queue_name = 'tx-sqs-tests'

    # Use boto to set up the queue and post messages for us to read. Lets us
    # concentrate on reading messages for now.
    module.boto_conn = boto.sqs.connect_to_region(region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
    module.boto_queue = module.boto_conn.create_queue(queue_name)
    module.boto_queue.clear()

    # This is our unit under test.
    module.receive_message = txsqs.ReceiveMessage(
            boto_queue.url, aws_access_key_id, aws_secret_access_key)


class TestReceiveMessage(object):

    @tt.deferred()
    @defer.inlineCallbacks
    def test_read_one_message_from_empty_queue(self):
        response = yield receive_message()
        nt.assert_is_none(response)

    @tt.deferred()
    @defer.inlineCallbacks
    def test_read_one_message_from_queue(self):
        message = boto_queue.new_message('Cumbersome Cucumber')
        boto_queue.write(message)
        response = yield receive_message()
        nt.assert_equals(response.body(), 'Cumbersome Cucumber')

    @tt.deferred()
    @defer.inlineCallbacks
    def test_read_many_messages_from_queue(self):
        for i in range(6):
            message = boto_queue.new_message(str(i))
            boto_queue.write(message)

        received = []
        for i in range(6):
            response = yield receive_message()
            received.append(int(response.body()))

        # SQS does not guarantee FIFO, so just check that we got all messages
        # regardless of order.
        nt.assert_equals(sum(received), 15)
