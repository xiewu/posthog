from ee.models.explicit_team_membership import ExplicitTeamMembership
from posthog.models.organization import OrganizationMembership

from posthog.models.user import User
from posthog.models.team import Team

from posthog.test.base import BaseTest


class TestTeam(BaseTest):
    def test_secret_api_token_generated_on_save(self):
        team = Team.objects.create(organization=self.organization, name="Test Team")
        team.secret_api_token = None
        team.secret_api_token_backup = None
        team.save()
        # Mypy gets confused here because it doesn't know that the `save` method is overridden and will populate the secret api tokens
        # It thinks the first assert will fail and thus the second one is unreachable. Silly mypy.
        secret_api_token = team.secret_api_token or ""
        secret_api_token_backup = team.secret_api_token_backup or ""
        assert secret_api_token.startswith("phs_")
        assert secret_api_token_backup.startswith("phs_")
        assert secret_api_token_backup != team.secret_api_token

    def test_secret_api_token_not_generated_if_already_set(self):
        team = Team.objects.create(organization=self.organization, name="Test Team")
        team.secret_api_token = "phs_test_token"
        team.secret_api_token_backup = "phs_test_token_backup"
        team.save()
        assert team.secret_api_token == "phs_test_token"
        assert team.secret_api_token_backup == "phs_test_token_backup"

    def test_all_users_with_access_simple_org_membership(self):
        self.organization_membership.level = OrganizationMembership.Level.MEMBER
        self.organization_membership.save()
        another_user = User.objects.create_and_join(self.organization, "test2@posthog.com", None)

        all_user_with_access_ids = list(self.team.all_users_with_access().values_list("id", flat=True))

        assert sorted(all_user_with_access_ids) == sorted([self.user.id, another_user.id])

    def test_all_users_with_access_simple_org_membership_and_redundant_team_one(self):
        self.organization_membership.level = OrganizationMembership.Level.MEMBER
        self.organization_membership.save()
        another_user = User.objects.create_and_join(self.organization, "test2@posthog.com", None)
        ExplicitTeamMembership.objects.create(team=self.team, parent_membership=self.organization_membership)

        all_user_with_access_ids = list(self.team.all_users_with_access().values_list("id", flat=True))

        assert sorted(all_user_with_access_ids) == sorted(
            [self.user.id, another_user.id]
        )  # self.user should only be listed once

    def test_all_users_with_access_while_access_control_org_membership(self):
        self.organization_membership.level = OrganizationMembership.Level.ADMIN
        self.organization_membership.save()
        self.team.access_control = True
        self.team.save()
        User.objects.create_and_join(
            self.organization, email="test2@posthog.com", password=None, level=OrganizationMembership.Level.MEMBER
        )

        all_user_with_access_ids = list(self.team.all_users_with_access().values_list("id", flat=True))

        assert all_user_with_access_ids == [self.user.id]  # The other user is only a plain member

    def test_all_users_with_access_while_access_control_explicit_team_membership(self):
        self.organization_membership.level = OrganizationMembership.Level.MEMBER
        self.organization_membership.save()
        self.team.access_control = True
        self.team.save()
        User.objects.create_and_join(
            self.organization, email="test2@posthog.com", password=None, level=OrganizationMembership.Level.MEMBER
        )
        ExplicitTeamMembership.objects.create(team=self.team, parent_membership=self.organization_membership)

        all_user_with_access_ids = list(self.team.all_users_with_access().values_list("id", flat=True))

        assert all_user_with_access_ids == [self.user.id]  # The other user is only a plain member

    def test_all_users_with_access_while_access_control_org_membership_and_redundant_team_one(self):
        self.organization_membership.level = OrganizationMembership.Level.ADMIN
        self.organization_membership.save()
        self.team.access_control = True
        self.team.save()
        ExplicitTeamMembership.objects.create(team=self.team, parent_membership=self.organization_membership)

        all_user_with_access_ids = list(self.team.all_users_with_access().values_list("id", flat=True))

        assert all_user_with_access_ids == [self.user.id]  # self.user should only be listed once
