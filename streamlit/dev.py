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

def message_component(message: Message, messages: list[Message]):
    """
    Displays a single message using Streamlit's elements.  Since Streamlit
    re-runs the entire script on interaction, we don't need to manage
    state within the component itself in the same way as React.  We
    use st.session_state to manage the list of messages.

    Args:
        message: The Message object to display.
        messages: The list of Message objects.
    """
    message_container = st.empty() # creates a placeholder

    if message in messages: # Check if the message is still in the list.
        with message_container.container():
            if message.type == "success":
                st.success(message.text)
            elif message.type == "warning":
                st.warning(message.text)
            elif message.type == "error":
                st.error(message.text)
            else:  # info
                st.info(message.text)

            if message.duration is not None and message.duration > 0:
                time.sleep(message.duration / 1000)  # Convert ms to seconds
                # Remove message.
                messages.remove(message)
                st.rerun() # rerun to remove message
            elif message.duration == 0:
                if st.button("Close", key=message.id):
                    messages.remove(message)
                    st.rerun()

def message_container(messages: list[Message]):
    """
    Displays all messages at the bottom of the page.  In Streamlit, we
    approximate "bottom of the page" by placing this at the very end
    of the script.  We iterate through the messages in session_state.

    Args:
        messages: The list of Message objects to display.
    """
    for message in messages:
        message_component(message, messages)

def add_message(messages: list[Message], message: Message):
    """
    Adds a message to the list in session_state.

    Args:
        message: The Message object to add.
    """
    messages.append(message)
    st.rerun() # important: force streamlit to re-run and show the message

def main():
    """
    Main function to run the Streamlit app.
    """
    st.title("Message Display App")
    st.write("Messages will appear at the bottom of the page.")

    # Initialize session_state for messages if it doesn't exist
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Example usage (for demonstration)
    if st.button("Show Success Message"):
        add_message(st.session_state.messages, Message("success", "Welcome! This is a success message.", 3000))
    if st.button("Show Info Message"):
        add_message(st.session_state.messages, Message("info", "This is an informational message.", 4000))
    if st.button("Show Warning Message"):
        add_message(st.session_state.messages, Message("warning", "Be careful! This is a warning.", 0))  # Stays until clicked
    if st.button("Show Error Message"):
        add_message(st.session_state.messages, Message("error", "An error occurred!", 5000))


if __name__ == "__main__":
    main()
