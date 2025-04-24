import streamlit as st
import time
from typing import Literal, Optional
from uuid import uuid4

MessageType = Literal["success", "warning", "error", "info"]

class Message:
    def __init__(self,
                 type: MessageType,
                 text: str,
                 duration: Optional[int] = 5000):
        self.id = str(uuid4())
        self.type = type
        self.text = text
        self.duration = duration

class NotificationCenter:
    """
    Handles the display of messages at the bottom of the page.
    """
    def __init__(self):
        self.notifications = []
        # Use a Streamlit session state to store notifications
        if 'notifications' not in st.session_state:
            st.session_state.notifications = []
        self.notifications = st.session_state.notifications

    def notification_component(self, notification: Message):
        """
        Displays a single notification using Streamlit's elements.
        """
        notification_container = st.empty()  # creates a placeholder

        if notification in self.notifications:  # Check if the notification is still in the list.
            with notification_container.container():
                if notification.type == "success":
                    st.success(notification.text)
                elif notification.type == "warning":
                    st.warning(notification.text)
                elif notification.type == "error":
                    st.error(notification.text)
                else:  # info
                    st.info(notification.text)

                if notification.duration is not None and notification.duration > 0:
                    time.sleep(notification.duration / 1000)  # Convert ms to seconds
                    # Remove notification.
                    self.notifications = [n for n in self.notifications if n.id != notification.id]
                    st.session_state.notifications = self.notifications
                    #st.rerun()  # Removed:  Do not rerun here.  Rerun in display_notifications
                elif notification.duration == 0:
                    if st.button("Close", key=notification.id):
                        self.notifications = [n for n in self.notifications if n.id != notification.id]
                        st.session_state.notifications = self.notifications
                        st.rerun()

    def display_notifications(self):
        """
        Displays all notifications. Call this at the end of the main script.
        """
        #st.write(self.notifications) #debug
        for notification in self.notifications:
            self.notification_component(notification)
        # Check for any expired notifications and remove them.  Do this *before* displaying
        self.notifications = [n for n in self.notifications if (time.time() - getattr(n, 'start_time', 0)) < (n.duration/1000) if n.duration is not None and n.duration > 0]
        st.session_state.notifications = self.notifications #update session state
        if len(self.notifications) != len(st.session_state.notifications):
            st.rerun() #rerun if changes

    def add_notification(self, notification: Message):
        """
        Adds a notification to the list.
        """
        notification.start_time = time.time() #set start time
        self.notifications.append(notification)
        st.session_state.notifications = self.notifications # persist
        #st.rerun()  # Removed:  Do not rerun here.