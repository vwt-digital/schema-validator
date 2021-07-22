def get_issue_information(msg_info, made_comments):
    comment = ""
    title = ""
    comment_info = ""
    comment_error = ""
    comment_schema_key = ""
    if msg_info["type"] == "message":
        title = "Messages not conform schema: topic '{}' schema '{}'".format(
            msg_info["topic_name"], msg_info["schema_tag"]
        )
        # Error information
        e = msg_info["error"]

        error_message = e.message
        error_absolute_schema_path = f"{list(e.absolute_schema_path)}"
        error_absolute_path = f"{list(e.absolute_path)}"
        error_value = f"{e.validator} '{e.validator_value}'"
        instance = f"{e.instance}"
        error = error_message.replace(f"{instance} ", "")
        # Check if the error was already commented in this session
        comment_info = {
            "error_absolute_path": error_absolute_path,
            "error_absolute_schema_path": error_absolute_schema_path,
            "error_value": error_value,
            "title": title,
        }
        # If it is, skip the message
        if comment_info in made_comments:
            return None, None, None, None, None, made_comments

        # Make comment
        comment_place = (
            f"Wrong message can be found in blob {msg_info['blob_full_name']}"
            + f" in history bucket {msg_info['history_bucket']}"
        )
        comment_error_msg_key = (
            f"\nThe error in the message can be found in key: {error_absolute_path}"
        )
        comment_error = f"\nThe error for this key is: {error}"
        comment_schema_key = f"\nIn the schema, the error can be found in key: {error_absolute_schema_path}"
        comment = (
            comment_place + comment_error_msg_key + comment_error + comment_schema_key
        )
    elif msg_info["type"] == "schema":
        title = "Schema not conform correct format: topic '{}' schema '{}'".format(
            msg_info["topic_name"], msg_info["schema_tag"]
        )
        # Error information
        e = msg_info["error"]

        error_message = e.message
        error_absolute_schema_path = f"{list(e.schema_path)}"
        error_value = f"{e.validator} '{e.validator_value}'"
        instance = f"{e.instance}"
        error = error_message.replace(f"{instance} ", "")
        # Check if the error was already commented in this session
        comment_info = {
            "error_absolute_path": None,
            "error_absolute_schema_path": error_absolute_schema_path,
            "error_value": error_value,
            "title": title,
        }
        # If it is, skip the message
        if comment_info in made_comments:
            return None, None, None, None, None, made_comments

        # Make comment
        comment = f"\nThe error for this schema is: {error} \nThe error can be found in key: {error_absolute_schema_path}"
    elif msg_info["type"] == "blob":
        title = "Blob could not be parsed: topic '{}' schema '{}'".format(
            msg_info["topic_name"], msg_info["schema_tag"]
        )
        # Error information
        e = msg_info["error"]

        comment_info = {
            "error_absolute_path": None,
            "error_absolute_schema_path": None,
            "error_value": e,
            "title": title,
        }
        made_comments.append(comment_info)

        comment_place = f"Wrong blob {msg_info['blob_full_name']} is in history bucket {msg_info['history_bucket']}"
        comment_error = f"\nThe error for parsing this blob is: {e}"
        comment = comment_place + comment_error
    else:
        return None, None, None, None, None, made_comments
    return (
        title,
        comment,
        comment_info,
        comment_error,
        comment_schema_key,
        made_comments,
    )
