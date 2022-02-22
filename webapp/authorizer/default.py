

class DefaultAuthorizer:
    """

    """

    def __init__(self):
        pass

    # ====================================================================
    # Main functions
    # ====================================================================
    def user_can_read(self, username, key):
        """
        Returns True if there is an access control rule that allows the
        user to read from the given key. If not, returns False.
        """
        return True

    def user_can_write(self, username, key):
        """
        Returns True if there is an access control rule that allows the
        user to write to the given key. If not, returns False.
        """
        return True

    def token_get(self, username):
        """
        Returns the user's token
        """
        return "password"

    # ====================================================================
    # Users
    # ====================================================================
    def user_create(self, username):
        """
        Creates a new user with the given username and creates a new token
        for the user.
        """
        return ""

    def user_delete(self, username):
        """
        Deletes the user with the given username. Deletes also the user's
        token and access control rules.
        """
        return

    def user_all(self):
        """
        Returns a list of all users
        """
        return

    # ====================================================================
    # Tokens
    # ====================================================================
    def token_create(self, username):
        """
        Creates a new token for the user (overwrites the old one)
        """
        return

    # ====================================================================
    # Access control
    # ====================================================================
    def access_control_rule_create(self, username, key, can_write=False):
        """
        Creates a new access control rule
        """
        return

    def access_control_rule_delete(self, username, key):
        """
        Deletes an existing access control rule
        """
        return

    def access_control_rule_all(self):
        """
        Returns a list with all access control rules
        """
        return
