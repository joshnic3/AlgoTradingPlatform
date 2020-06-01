from library.bootstrap import Constants
from library.interfaces.sql_database import Database, query_result_to_dict
from hashlib import sha256
from library.utilities.onboarding import generate_unique_id

# user_id + password -> user_hash
# this way profile names can hot swapped.

# "profiles": ["id", "username", "hash"]
# "strategy_profiles": ["id", "profile_id", "strategy_id"]

# c1b86e8a071d89f698006c91d943a7cd

BASE = 5
POWER = 23


def generate_user_hash(username, password):
    string_to_hash = (username + password).encode()
    hash_object = sha256(string_to_hash)
    return hash_object.hexdigest()


def authenticate_profile(username, user_hash):
    # Check hash is valid
    db = Database(name='user_data')
    profile_row = db.get_one_row('profiles', 'username="{}"'.format(username))
    if not profile_row:
        return None
    profile_dict = query_result_to_dict([profile_row], Constants.configs['tables']['user_data']['profiles'])[0]
    if user_hash == profile_dict['hash']:
        return profile_dict['id']
    else:
        return None


def add_profile(username, user_hash):
    profile_id = generate_unique_id(username)
    db = Database(name='user_data')
    values = [profile_id, username, user_hash]
    db.insert_row('profiles', values)
    return profile_id


# -----------------------------------------------------------

def is_whitelisted(ip_address):
    return True


def public_key_from_private_key(private_key):
    return (BASE ** private_key) % POWER


def secret_key(public_key, private_key):
    return (public_key ** private_key) % POWER
