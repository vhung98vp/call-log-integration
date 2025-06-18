from utils import is_spam_number, check_relation, build_output_message

def main(agg_list, metalist_A, phone_A, phone_B):
    if is_spam_number(phone_A, metalist_A):
        # Send spam number to list or ignore and don't check relation
        return False

    if check_relation(agg_list, metalist_A, phone_A, phone_B):
        message = build_output_message(phone_A, phone_B)
        # Send output message to Kafka
        return True
    return False
