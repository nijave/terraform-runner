import re

from colors import strip_color

from terraform.types import TerraformPlanOutput


class PlanTextAnalyzer:
    @staticmethod
    def has_changes(plan_text: TerraformPlanOutput) -> bool:
        """
        Returns whether plan text indicates there are changes
        :param plan_text: output from "terraform plan"
        :return: whether there are changes
        """
        # An execution plan has been generated and is shown below.
        # Resource actions are indicated with the following symbols:
        #     ~ update in-place
        #     - destroy
        # <= read (data resources)
        #
        # Terraform will perform the following actions:
        return "Terraform will perform the following actions:" in (
            strip_color(line).strip() for line in reversed(plan_text.splitlines())
        )

    @staticmethod
    def changes(plan_text: TerraformPlanOutput) -> str:
        """
        Extracts the change report from terraform plan
        console output
        :param plan_text: console plan output
        :return: plan text section
        """
        lines = plan_text.splitlines()
        clean_lines = [strip_color(line).strip() for line in lines]

        try:
            plan_start_line = next(
                i
                for i in range(len(clean_lines))
                if re.fullmatch(r"-+", clean_lines[i])
            )
        except StopIteration:
            raise ValueError("Couldn't find change start marker line")

        return "\n".join(lines[plan_start_line + 2 :])

    @staticmethod
    def change_summary(plan_text: TerraformPlanOutput) -> str:
        """
        A summary with only changing attributes like
        terraform-landscape
        :param plan_text:
        :return:
        """

        summary = "\n".join(
            line
            for line in plan_text.splitlines()
            if len((clean_line := strip_color(line)).strip()) > 0
            # Starts with spaces and a change indicator or a bold #
            and (re.search(r"\s{2,}[~+-]", clean_line) or line.startswith("\x1b[1m  #"))
        ).replace("\x1b[1m  #", "\n\x1b[1m  #")

        return summary.strip()
