import json
import logging

import config
from gobits import Gobits
from google.cloud import pubsub_v1

import atlassian
import secretmanager


def create_issue(msg_info, made_comments):
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


def check_title(
    titles,
    title,
    msg_info,
    client,
    jira_project,
    made_comments,
    comment_info,
    comment,
    sprint_id,
    request,
    jira_projects_list,
    comment_error_msg_key,
    comment_error,
    comment_schema_key,
):
    if title not in titles:
        description = (
            f"The topic `{msg_info['topic_name']}` received messages"
            + f" that are not conform its schema ({msg_info['schema_tag']})."
            + " The messages with their errors can be found in the comments of this ticket"
            + " Please check why the messages are not conform the schema. "
        )
        logging.info(f"Creating jira ticket: {title}")
        # Create a Jira ticket
        issue = atlassian.create_issue(
            client=client,
            project=jira_project,
            title=title,
            description=description,
        )
        # Add comment to jira ticket
        made_comments.append(comment_info)
        atlassian.add_comment(client, issue, comment)
        # Add Jira ticket to sprint
        atlassian.add_to_sprint(client, sprint_id, issue.key)

        # Publish message to topic
        # Temporary until full move to formatting/interface
        metadata = Gobits.from_request(request=request).to_json()
        publisher = pubsub_v1.PublisherClient()
        prep_message = {"gobits": [metadata], "info": description}

        if config.TOPIC_NAME:
            future = publisher.publish(
                config.TOPIC_NAME, json.dumps(prep_message).encode("utf-8")
            )
            logging.info(f"Published message with id {future.result()}")
    # If it does exist, add a comment with the message and its error
    else:
        # Check if the error message has not already been created in this session
        made_comments = comment_exists_check(
            comment_info,
            made_comments,
            title,
            jira_projects_list,
            client,
            comment,
            comment_error_msg_key,
            comment_error,
            comment_schema_key,
        )
    return made_comments


def comment_exists_check(
    comment_info,
    made_comments,
    title,
    jira_projects_list,
    client,
    comment,
    comment_error_msg_key,
    comment_error,
    comment_schema_key,
):
    if comment_info not in made_comments:
        # Add comment to made comments in this session
        made_comments.append(comment_info)
        # Replace '-' in last part of title, otherwise JIRA does not see issues
        jql_title_list = title.split(":")
        jql_title_list[-1] = jql_title_list[-1].replace("-", " ")
        jql_title = ":".join(jql_title_list)
        # Get issues with title
        jql_prefix_titles = (
            f"type = Bug AND status != Done AND status != Cancelled "
            f'AND text ~ "{jql_title}" '
            "AND project = "
        )
        projects_titles = [
            jql_prefix_titles + project for project in jira_projects_list
        ]
        jql_titles = " OR ".join(projects_titles)
        jql_titles = f"{jql_titles} ORDER BY priority DESC "
        issues = atlassian.list_issues(client, jql_titles)

        # For every issue with this title
        for issue in issues:
            # Get comments of issues
            issue_id = atlassian.get_issue_id(client, issue)
            issue_comment_ids = atlassian.list_issue_comment_ids(client, issue_id)
            comment_not_yet_exists = True
            for comment_id in issue_comment_ids:
                # Check if the comment without where to find it does not yet exist
                comment_body = atlassian.get_comment_body(client, issue, comment_id)
                if repr(comment_body) == repr(comment):
                    comment_not_yet_exists = False
                    break
                if comment_error_msg_key and comment_error and comment_schema_key:
                    if comment_error_msg_key:
                        if (
                            comment_error_msg_key in comment_body
                            and comment_error in comment_body
                            and comment_schema_key in comment_body
                        ):
                            comment_not_yet_exists = False
                            break
            if comment_not_yet_exists:
                logging.info(f"Updating jira ticket: {title}")
                # Add comment to jira ticket
                atlassian.add_comment(client, issue_id, comment)
    return made_comments


def create_jira_tickets(messages_not_conform_schema, project_id, request):
    # Jira config
    jira_user = config.JIRA_USER
    jira_server = config.JIRA_SERVER
    jira_project = config.JIRA_PROJECT
    jira_projects = config.JIRA_PROJECTS
    jira_board = config.JIRA_BOARD
    jira_api_key = secretmanager.get_secret(project_id, config.JIRA_SECRET_ID)

    client = atlassian.jira_init(jira_user, jira_api_key, jira_server)

    # Get current sprint
    sprint_id = atlassian.get_current_sprint(client, jira_board)

    jira_projects_list = jira_projects.split("+")
    logging.info(
        f"Possibly creating or updating tickets for sprint {sprint_id} of projects {jira_projects_list}..."
    )

    # Jira jql to find tickets that already exist conform these issues
    jql_prefix = (
        'type = Bug AND status != Done AND status != "For Review" AND status != "For Review" AND status != Cancelled '
        'AND text ~ "Message not conform schema" '
        "AND project = "
    )
    projects = [jql_prefix + project for project in jira_projects_list]
    jql = " OR ".join(projects)
    jql = f"{jql} ORDER BY priority DESC "

    made_comments = []
    # For every message that is not conform the schema of its topic
    for msg_info in messages_not_conform_schema:
        comment_error_msg_key = None
        # Make issue
        (
            title,
            comment,
            comment_info,
            comment_error,
            comment_schema_key,
            made_comments,
        ) = create_issue(msg_info, made_comments)
        if not title:
            continue
        # Get issues that are already conform the 'issue template'
        titles = atlassian.list_issue_titles(client, jql)
        # Check if Jira ticket already exists for this topic with this schema
        made_comments = check_title(
            titles,
            title,
            msg_info,
            client,
            jira_project,
            made_comments,
            comment_info,
            comment,
            sprint_id,
            request,
            jira_projects_list,
            comment_error_msg_key,
            comment_error,
            comment_schema_key,
        )
