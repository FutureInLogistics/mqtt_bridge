cmake_minimum_required(VERSION 3.0.2)
project(mqtt_bridge_pkg)

find_package(catkin REQUIRED COMPONENTS message_generation std_msgs)
catkin_python_setup()

add_service_files(FILES
  SyncTopic.srv
  SyncTransform.srv
  ConditionalSyncTransform.srv
  Unsync.srv
)
generate_messages()

catkin_package(CATKIN_DEPENDS message_runtime)

install(PROGRAMS
  scripts/mqtt_bridge_node.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

install(DIRECTORY config
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

add_custom_target(install_depends ALL COMMAND "pip3" "install" "--user" "-r" "${PROJECT_SOURCE_DIR}/requirements.txt")
