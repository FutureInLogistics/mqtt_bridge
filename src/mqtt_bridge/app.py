import inject
import paho.mqtt.client as mqtt
import rospy

from .bridge import create_bridge, MqttToRosBridge, RosToMqttBridge
from .mqtt_client import create_private_path_extractor
from .util import lookup_object

from mqtt_bridge_pkg.srv import Subscribe, Unsubscribe
from std_msgs.msg import String

bridges = []

def create_config(mqtt_client, serializer, deserializer, mqtt_private_path):
    if isinstance(serializer, str):
        serializer = lookup_object(serializer)
    if isinstance(deserializer, str):
        deserializer = lookup_object(deserializer)
    private_path_extractor = create_private_path_extractor(mqtt_private_path)
    def config(binder):
        binder.bind('serializer', serializer)
        binder.bind('deserializer', deserializer)
        binder.bind(mqtt.Client, mqtt_client)
        binder.bind('mqtt_private_path_extractor', private_path_extractor)
    return config


def mqtt_bridge_node():
    # init node
    rospy.init_node('mqtt_bridge_node')

    # load parameters
    params = rospy.get_param("~", {})
    mqtt_params = params.pop("mqtt", {})
    conn_params = mqtt_params.pop("connection")
    mqtt_private_path = mqtt_params.pop("private_path", "")
    bridge_params = params.get("bridge", [])

    # create mqtt client
    mqtt_client_factory_name = rospy.get_param(
        "~mqtt_client_factory", ".mqtt_client:default_mqtt_client_factory")
    mqtt_client_factory = lookup_object(mqtt_client_factory_name)
    mqtt_client = mqtt_client_factory(mqtt_params)

    # load serializer and deserializer
    serializer = params.get('serializer', 'msgpack:dumps')
    deserializer = params.get('deserializer', 'msgpack:loads')

    # dependency injection
    config = create_config(
        mqtt_client, serializer, deserializer, mqtt_private_path)
    inject.configure(config)

    # configure and connect to MQTT broker
    mqtt_client.on_connect = _on_connect
    mqtt_client.on_disconnect = _on_disconnect
    mqtt_client.connect(**conn_params)

    # configure bridges
    for bridge_args in bridge_params:
        bridges.append(create_bridge(**bridge_args))

    # start MQTT loop
    mqtt_client.loop_start()

    # create service for dynamically creating new bridges in the future
    rospy.Service('mqtt_bridge_subscribe', Subscribe, _on_subscribe)
    rospy.Service('mqtt_bridge_unsubscribe', Unsubscribe, _on_unsubscribe)

    # register shutdown callback and spin
    rospy.on_shutdown(mqtt_client.disconnect)
    rospy.on_shutdown(mqtt_client.loop_stop)
    rospy.spin()


def _on_connect(client, userdata, flags, response_code):
    rospy.loginfo('MQTT connected')


def _on_disconnect(client, userdata, response_code):
    rospy.loginfo('MQTT disconnected')


def _on_subscribe(req):
    """
    Subscribe service callback function.
    
    This function creates a new MQTT->ROS or ROS->MQTT bridge, depending on the 
    'publish' flag in the service request.
    """
    try:
        if req.publish:
            bridge = create_bridge("mqtt_bridge.bridge:RosToMqttBridge", req.msg_type, req.ros_topic, req.mqtt_topic)
        else:
            bridge = create_bridge("mqtt_bridge.bridge:MqttToRosBridge", req.msg_type, req.mqtt_topic, req.ros_topic)
    except Exception as Err:
        rospy.roserror(str(Err))
        return False
    else:
        bridges.append(bridge)
        return True


def _on_unsubscribe(req):
    """
    Unsubscribe service callback function.
    The only parameter is the name of the MQTT or ROS topic to unsubscribe from, as String

    Note that this service can stop the data flow in both directions, as long as the topic name matches.
    """

    for i, bridge in enumerate(bridges):
        if bridge._topic_from == req.topic:
            if isinstance(bridge, MqttToRosBridge):
                # Unsubscribe the MQTT client
                bridge._mqtt_client.unsubscribe(bridge._topic_from)
                print("Unsubscribed from MQTT topic %s" % bridge._topic_from)

            elif isinstance(bridge, RosToMqttBridge):
                # Unsubscribe the ROS subscriber
                bridge._ros_subscriber.unregister()
                print("Unsubscribed from ROS topic %s" % bridge._topic_from)

            # Remove bridge from global list
            del bridges[i]

__all__ = ['mqtt_bridge_node']
