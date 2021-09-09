from abc import ABCMeta
from typing import Optional, Type, Dict, Union

import inject
import paho.mqtt.client as mqtt
import rospy

from .util import lookup_object, extract_values, populate_instance


def create_bridge(factory: Union[str, "Bridge"], msg_type: Union[str, Type[rospy.Message]], topic_from: str,
                  topic_to: str, frequency: Optional[float] = None, **kwargs) -> "Bridge":
    """ generate bridge instance using factory callable and arguments. if `factory` or `meg_type` is provided as string,
     this function will convert it to a corresponding object.
    """
    if isinstance(factory, str):
        factory = lookup_object(factory)
    if not issubclass(factory, Bridge):
        raise ValueError("factory should be Bridge subclass")
    if isinstance(msg_type, str):
        msg_type = lookup_object(msg_type)
    if not issubclass(msg_type, rospy.Message):
        raise TypeError(
            "msg_type should be rospy.Message instance or its string"
            "reprensentation")
    return factory(
        topic_from=topic_from, topic_to=topic_to, msg_type=msg_type, frequency=frequency, **kwargs)


class Bridge(object, metaclass=ABCMeta):
    """ Bridge base class """
    _mqtt_client = inject.attr(mqtt.Client)
    _serialize = inject.attr('serializer')
    _deserialize = inject.attr('deserializer')
    _extract_private_path = inject.attr('mqtt_private_path_extractor')


class RosToMqttBridge(Bridge):
    """ Bridge from ROS topic to MQTT

    bridge ROS messages on `topic_from` to MQTT topic `topic_to`. expect `msg_type` ROS message type.
    """

    def __init__(self, topic_from: str, topic_to: str, msg_type: rospy.Message, frequency: Optional[float] = None,
                latch : bool = False, condition = None):
        rospy.loginfo("Bridging ROS -> MQTT: " + topic_from + " ~~> " + topic_to)
        self._topic_from = topic_from
        self._topic_to = self._extract_private_path(topic_to)
        self._latch = latch
        self._condition = condition
        self._last_published = rospy.get_time()
        self._interval = 0 if frequency is None else 1.0 / frequency
        self._ros_subscriber = rospy.Subscriber(topic_from, msg_type, self._callback_ros)

    def _callback_ros(self, msg: rospy.Message):
        rospy.logdebug("ROS received from {}".format(self._topic_from))
        now = rospy.get_time()
        if now - self._last_published >= self._interval:
            if self._condition is None or self._condition(msg):
                self._publish(msg)
                self._last_published = now

    def _publish(self, msg: rospy.Message):
        payload = self._serialize(extract_values(msg))
        self._mqtt_client.publish(topic=self._topic_to, payload=payload, retain=self._latch)


class MqttToRosBridge(Bridge):
    """ Bridge from MQTT to ROS topic

    bridge MQTT messages on `topic_from` to ROS topic `topic_to`. MQTT messages will be converted to `msg_type`.
    """

    def __init__(self, topic_from: str, topic_to: str, msg_type: Type[rospy.Message],
                 frequency: Optional[float] = None, queue_size: int = 10, latch : bool = False,
                 sync_to_clock : str = None):
        rospy.loginfo("Bridging MQTT -> ROS: " + topic_from + " ~~> " + topic_to)
        self._topic_from = self._extract_private_path(topic_from)
        self._topic_to = topic_to
        self._msg_type = msg_type
        self._queue_size = queue_size
        self._latch = latch
        self._last_published = rospy.get_time()
        self._interval = None if frequency is None else 1.0 / frequency
        # Adding the correct topic to subscribe to
        self._mqtt_client.subscribe(self._topic_from)
        self._mqtt_client.message_callback_add(self._topic_from, self._callback_mqtt)
        self._publisher = rospy.Publisher(
            self._topic_to, self._msg_type, queue_size=self._queue_size, latch=self._latch)
        self._clock_diff = None

        # Clock syncing option
        if sync_to_clock != None and len(sync_to_clock) > 0:
            def _calc_clock_diff(client: mqtt.Client, userdata: Dict, mqtt_msg: mqtt.MQTTMessage):
                #if self._clock_diff != None:
                #    return

                # Calculate clock difference and save to instance attribute
                clock_msg_type = lookup_object("rosgraph_msgs.msg:Clock")
                clock_msg = self._create_ros_message(mqtt_msg, clock_msg_type)
                self._clock_diff = rospy.Time.now() - clock_msg.clock
                #rospy.loginfo("CLOCK SYNC: Calculated extern clock offset of %u.%u",
                #    self._clock_diff.secs,
                #    self._clock_diff.nsecs)

                # Getting the clock diff once is enough, so unsubscribe
                #self._mqtt_client.unsubscribe(sync_to_clock)

            # Subscribe to clock of partner skid
            self._mqtt_client.subscribe(sync_to_clock)
            self._mqtt_client.message_callback_add(sync_to_clock, _calc_clock_diff)

    def _callback_mqtt(self, client: mqtt.Client, userdata: Dict, mqtt_msg: mqtt.MQTTMessage):
        """ callback from MQTT """
        rospy.logdebug("MQTT received from {}".format(mqtt_msg.topic))
        now = rospy.get_time()

        if self._interval is None or now - self._last_published >= self._interval:
            try:
                ros_msg = self._create_ros_message(mqtt_msg, self._msg_type)

                if self._topic_to == "/tf":
                    for i in range(len(ros_msg.transforms)):
                        if self._clock_diff != None:
                            ros_msg.transforms[i].header.stamp += self._clock_diff + rospy.Duration.from_sec(0.01)

                        # This transform needs to be inverted
                        original_parent = ros_msg.transforms[i].header.frame_id
                        original_child = ros_msg.transforms[i].child_frame_id 
                        ros_msg.transforms[i].header.frame_id = original_child
                        ros_msg.transforms[i].child_frame_id = original_parent                    

                if self._clock_diff != None:
                    if hasattr(ros_msg, "header"):
                        ros_msg.header.stamp += self._clock_diff

                self._publisher.publish(ros_msg)
                self._last_published = now
            except Exception as e:
                rospy.logerr(e)

    def _create_ros_message(self, mqtt_msg: mqtt.MQTTMessage, msg_type : Type[rospy.Message]) -> rospy.Message:
        """ create ROS message from MQTT payload """
        # Hack to enable both, messagepack and json deserialization.
        if self._serialize.__name__ == "packb":
            msg_dict = self._deserialize(mqtt_msg.payload, raw=False)
        else:
            msg_dict = self._deserialize(mqtt_msg.payload)
        return populate_instance(msg_dict, msg_type())


__all__ = ['create_bridge', 'Bridge', 'RosToMqttBridge', 'MqttToRosBridge']
