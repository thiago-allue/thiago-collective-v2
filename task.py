"""
Note that this example WILL NOT RUN in CoderPad, since it does not provide a Django environment in which to run.
This is an extracted sample from production (at one point).
"""


"""
See Thiago's notes here: https://tinyurl.com/2p82zwx2
"""

# from simple_history.models import HistoricalRecords
import datetime
from datetime import datetime

from dateutil.relativedelta import relativedelta
from django import db
# Excerpted from `hyke.api.models`...
from django.db import models
from django.db.models import Q
from django.utils import timezone
from hyke.api.models import EmailView
###
# Excerpted below:
# from hyke.api.models import (
#    ProgressStatus,
#    StatusEngine,
# )
##
from hyke.automation.jobs import (
    nps_calculator_onboarding,
    nps_calculator_running,
)
from hyke.email.jobs import send_transactional_email
from hyke.fms.jobs import create_dropbox_folders
from hyke.scheduled.base import next_annualreport_reminder
from hyke.scheduled.service.nps_surveys import (
    schedule_next_running_survey_sequence,
    schedule_onboarding_survey_sequence,
    send_client_onboarding_survey,
)


class ProgressStatus(models.Model):
    PENDING = "pending"
    COMPLETED = "completed"

    email = models.CharField(max_length=100, default="---")
    llc_formation_status = models.CharField(max_length=50, default="---")
    post_formation_status = models.CharField(max_length=50, default="---")
    e_in_status = models.CharField(max_length=50, default="---")
    business_license_status = models.CharField(max_length=50, default="---")
    bank_account_status = models.CharField(max_length=50, default="---")
    contribution_status = models.CharField(max_length=50, default="---")
    SOI_status = models.CharField(max_length=50, default="---")
    FTB_status = models.CharField(max_length=50, default="---")
    questionnaire_status = models.CharField(max_length=50, default="---")
    bookkeeping_setup_status = models.CharField(max_length=50, default="---")
    tax_setup_status = models.CharField(max_length=50, default="---")
    client_survey_status = models.CharField(max_length=50, default="---")
    bk_services_setup_status = models.CharField(
        max_length=50, choices=[(PENDING, PENDING), (COMPLETED, COMPLETED), ], default=PENDING
    )

    # history = HistoricalRecords()

    class Meta:
        verbose_name = "ProgressStatus"
        verbose_name_plural = "ProgressStatuses"

    def __str__(self):
        return "%s - %s" % (self.id, self.email)


class StatusEngine(models.Model):
    FAILED = -4
    SECOND_RETRY = -3
    FIRST_RETRY = -2
    SCHEDULED = -1
    COMPLETED = 1
    UNNECESSARY = 4
    OFF_BOARDED = 5

    OUTCOMES = [
        (SCHEDULED, "Scheduled"),
        (COMPLETED, "Completed"),
        (UNNECESSARY, "Cancelled due to Completed Task"),
        (OFF_BOARDED, "Cancelled due to Offboarding"),
        (FIRST_RETRY, "Retrying previously failed"),
        (SECOND_RETRY, "Retrying previously failed again"),
        (FAILED, "Gave up retrying due to multiple failures"),
    ]

    email = models.CharField(max_length=50, blank=True)
    process = models.CharField(max_length=100)
    formation_type = models.CharField(max_length=20, default="---")
    process_state = models.IntegerField(default=1)
    outcome = models.IntegerField(choices=OUTCOMES, default=SCHEDULED)
    data = models.CharField(max_length=1000, default="---")
    created = models.DateTimeField(default=timezone.now)
    executed = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = "StatusEngine"
        verbose_name_plural = "StatusEngines"

    def __str__(self):
        return "%s - %s - %s" % (self.id, self.email, self.process)


# END Excerpt of `hyke.api.models`

from constants import *
from structlog import get_logger


logger = get_logger(__name__)


# Separate the tasks into functions
def handle_client_onboarding_survey(item):
    if item.process_state == 1 and item.outcome == -1:
        try:
            send_client_onboarding_survey(email=item.email)
        except Exception:  # Specify exception clause. See [3] in "Notes Collective.gdoc"
            logger.exception(f"Can't process Onboarding NPS Survey for status engine id={item.id}")


def handle_payment_error_email(item):
    if item.process_state == 1 and item.outcome == -1:
        send_transactional_email(email=item.email,
                                 template="[Action required] - Please update your payment information")
        print("[Action required] - Please update your payment information. An email was sent to " + item.email)


def handle_running_flow(item):
    if item.process_state == 1 and item.outcome == -1:
        ps = ProgressStatus.objects.get(email=item.email)
        ps.bookkeeping_setup_status = "completed"
        ps.tax_setup_status = "completed2"
        ps.save()

        StatusEngine.objects.get_or_create(
            email=item.email,
            process="Schedule Email",
            formationtype=HYKE_DAILY,
            processstate=1,
            outcome=StatusEngine.SCHEDULED,
            data="What's upcoming with Collective?",
            defaults={"executed": timezone.now() + relativedelta(days=1)},
        )

        StatusEngine.objects.get_or_create(
            email=item.email,
            process="Running flow",
            formationtype=HYKE_DAILY,
            processstate=2,
            defaults={"outcome": StatusEngine.SCHEDULED, "data": "---"},
        )

        schedule_onboarding_survey_sequence(email=item.email)
        schedule_next_running_survey_sequence(email=item.email)

        create_dropbox_folders(email=item.email)

        print("Dropbox folders are created for " + item.email)

        has_run_before = StatusEngine.objects.filter(
            email=item.email, process=item.process, processstate=item.process_state, outcome=1,
        ).exists()

        if has_run_before:
            print(
                "Not creating form w9 or emailing pops because dropbox folders job has already run for {}".format(
                    item.email
                )
            )
    elif item.process_state == 2 and item.outcome == -1:
        email_views = EmailView.objects.filter(type="annual", legacy=True)
        for ev in email_views:
            template_date = ev.date.split("-")
            email_date = datetime.now()
            email_date = email_date.replace(month=int(template_date[0]), day=int(template_date[1]))
            email_date = email_date.replace(hour=23, minute=59, second=59)

            se = StatusEngine(
                email=item.email,
                process="Reminder",
                formationtype=HYKE_DAILY,
                processstate=1,
                outcome=-1,
                data=ev.title,
                executed=email_date,
            )
            se.save()


def handle_annual_report_uploaded(item):
    if item.outcome == -1:

        report_details = item.data.split("---")
        report_name = report_details[1].strip()
        report_year = report_details[0].strip()
        report_state = report_details[2].strip() if len(report_details) == 3 else None

        data_filter = Q(data=f"{report_year} --- {report_name}")
        if report_state:
            data_filter |= Q(data=f"{report_year} --- {report_name} --- {report_state}")

        status_engines = StatusEngine.objects.filter(email=item.email,
                                                     process="Annual Report Reminder", outcome=-1).filter(
            data_filter
        )
        for se in status_engines:
            se.outcome = 1
            se.executed = timezone.now()
            se.save()

        # complete this before we schedule the next reminder
        item.outcome = StatusEngine.COMPLETED
        item.executed = timezone.now()
        item.save()

        next_annualreport_reminder(item.email, report_name, report_state)


def handle_calculate_nps_running(item):
    if item.outcome == -1:
        nps_calculator_running()
        print("Running NPS is calculated for " + item.data)


def handle_calculate_nps_onboarding(item):
    if item.outcome == -1:
        nps_calculator_onboarding()
        print("Onboarding NPS is calculated for " + item.data)


def handle_kickoff_questionnaire_completed(item):
    if item.process_state == 1 and item.outcome == -1:
        progress_status = ProgressStatus.objects.filter(email__iexact=item.email).first()
        if progress_status:
            progress_status.questionnaire_status = "scheduled"
            progress_status.save()

            StatusEngine.objects.create(
                email=item.email,
                processstate=1,
                formationtype=HYKE_SALESFORCE,
                outcome=-1,
                process="Kickoff Questionnaire Completed",
                data=item.data,
            )


def handle_kickoff_call_scheduled(item):
    if item.process_state == 1 and item.outcome == -1:
        progress_status = ProgressStatus.objects.get(email__iexact=item.email)
        progress_status.questionnaire_status = "scheduled"
        progress_status.save()

        StatusEngine.objects.create(
            email=item.email,
            processstate=1,
            formationtype=HYKE_SALESFORCE,
            outcome=-1,
            process="Kickoff Call Scheduled",
            data=item.data,
        )


def handle_kickoff_call_cancelled(item):
    if item.process_state == 1 and item.outcome == -1:
        progress_status = ProgressStatus.objects.get(email__iexact=item.email)
        progress_status.questionnaire_status = "reschedule"
        progress_status.save()

        StatusEngine.objects.create(
            email=item.email,
            processstate=1,
            formationtype=HYKE_SALESFORCE,
            outcome=-1,
            process="Kickoff Call Cancelled",
        )


def handle_transition_plan_submitted(item):
    if item.process_state == 1 and item.outcome == StatusEngine.SCHEDULED:
        progress_status = ProgressStatus.objects.get(email__iexact=item.email)
        progress_status.questionnaire_status = "submitted"
        progress_status.save()

        StatusEngine.objects.create(
            email=item.email,
            process="Transition Plan Submitted",
            formationtype=HYKE_SALESFORCE,
            processstate=1,
            outcome=StatusEngine.SCHEDULED,
            data="---",
        )

        StatusEngine.objects.get_or_create(
            email=item.email,
            process="Schedule Email",
            formationtype=HYKE_DAILY,
            processstate=1,
            outcome=StatusEngine.SCHEDULED,
            data="Welcome to the Collective community!",
            defaults={"executed": timezone.now() + relativedelta(days=1)},
        )


def handle_bk_training_call_scheduled(item):
    if item.process_state == 1 and item.outcome == -1:
        StatusEngine.objects.create(
            email=item.email,
            processstate=1,
            formationtype=HYKE_SALESFORCE,
            outcome=-1,
            process="BK Training Call Scheduled",
            data=item.data,
        )


def handle_bk_training_call_cancelled(item):
    if item.process_state == 1 and item.outcome == -1:
        progress_status = ProgressStatus.objects.get(email__iexact=item.email)
        progress_status.bookkeeping_setup_status = "reschedule"
        progress_status.save()

        status_engine = StatusEngine(
            email=item.email,
            process="Followup - BK Training",
            formationtype=HYKE_DAILY,
            processstate=1,
            outcome=-1,
            data="---",
            executed=timezone.now() + relativedelta(days=2),
        )
        status_engine.save()

        StatusEngine.objects.create(
            email=item.email,
            processstate=1,
            formationtype=HYKE_SALESFORCE,
            outcome=-1,
            process="BK Training Call Cancelled",
        )

        # Add additional processing...
        item.outcome = 1
        item.executed = timezone.now()
        item.save()


def handle_bank_connect(item):
    if item.process_state == 1 and item.outcome == -1:
        send_transactional_email(
            email=item.email,
            template="SP.ONB.0021 - Account is created before, bank is connected later",
        )
        print("SP.ONB.0021 - Account is created before, bank is connected later, email is sent to " + item.email)

        item.outcome = 1
        item.executed = timezone.now()
        item.save()

    elif item.process_state == 2 and item.outcome == -1:
        send_transactional_email(
            email=item.email,
            template="SP.ONB.0010 - Account is created",
        )
        print("SP.ONB.0010 - Account is created email is being sent to " + item.email)

        item.outcome = 1
        item.executed = timezone.now()
        item.save()

    elif item.process_state == 3 and item.outcome == -1:
        if item.executed is None:
            ref_time = item.created
        else:
            ref_time = item.executed

        passed_seconds = (datetime.datetime.now(timezone.utc) - ref_time).total_seconds()

        if passed_seconds < 259200:
            return

        print(str(int(passed_seconds)) + " seconds passed...")

        send_transactional_email(
            email=item.email,
            template="SP.BNK.0010 - Please connect your bank (remind every 3 days)",
        )
        print("SP.BNK.0010 - Please connect your bank (remind every 3 days) email is sent to " + item.email)

        item.outcome = 1
        item.executed = timezone.now()
        item.save()


# Mapping
process_mapping = {
    CLIENT_ONBOARDING_SURVEY: handle_client_onboarding_survey,
    PAYMENT_ERROR_EMAIL: handle_payment_error_email,
    RUNNING_FLOW: handle_running_flow,
    ANNUAL_REPORT_UPLOADED: handle_annual_report_uploaded,
    CALCULATE_NPS_RUNNING: handle_calculate_nps_running,
    CALCULATE_NPS_ONBOARDING: handle_calculate_nps_onboarding,
    KICKOFF_QUESTIONNAIRE_COMPLETED: handle_kickoff_questionnaire_completed,
    KICKOFF_CALL_SCHEDULED: handle_kickoff_call_scheduled,
    KICKOFF_CALL_CANCELLED: handle_kickoff_call_cancelled,
    TRANSITION_PLAN_SUBMITTED: handle_transition_plan_submitted,
    BK_TRAINING_CALL_SCHEDULED: handle_bk_training_call_scheduled,
    BK_TRAINING_CALL_CANCELLED: handle_bk_training_call_cancelled,
    BANK_CONNECT: handle_bank_connect
}


def scheduled_system():
    print("Scheduled task has been started for Hyke System...")

    items = StatusEngine.objects.filter(Q(outcome=-1) & Q(formationtype__startswith=HYKE_DAILY))

    print("Active items in the job: " + str(len(items)))

    db.close_old_connections()

    for item in items:
        if item.process in process_mapping:
            process_mapping[item.process](item)

    print("Scheduled task is completed for Hyke System...\n")


if __name__ == "__main__":
    scheduled_system()
