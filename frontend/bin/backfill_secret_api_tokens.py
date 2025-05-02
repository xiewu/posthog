from posthog.models.team import Team
from django.db.models import Q

from django.db import transaction


def backfill_secret_api_tokens_in_batches(batch_size=1000):
    last_pk = 0
    while True:
        batch = list(
            Team.objects.filter(Q(secret_api_token=None) | Q(secret_api_token_backup=None), pk__gt=last_pk).order_by(
                "pk"
            )[:batch_size]
        )

        if not batch:
            break

        with transaction.atomic():
            for team in batch:
                # The `save` method is overridden to generate secret api tokens if they are not set
                team.save()

        last_pk = batch[-1].pk


backfill_secret_api_tokens_in_batches()
