"""IAM denials that carry a troubleshooter link, without the parts that change per request."""

from gcp_validation.errors import detailed
from gcp_validation.live_rest import Observations, Response

REMEDIATION = ' Remediate access with this Troubleshooter URL'
VARYING = ('error_info_id', 'troubleshooter_url')


def iam_denial(observe: Observations, response: Response) -> dict:
    """Select a denial as `detailed` does, minus its troubleshooter link.

    Artifact Registry and Vertex AI append a link with a per-request error ID to the
    message and put the ID and link in the ErrorInfo metadata.
    """
    selected = detailed(observe, response)
    selected['message'] = selected['message'].split(REMEDIATION, 1)[0]
    for detail in selected['details']:
        for key in VARYING:
            detail.get('metadata', {}).pop(key, None)
    return selected
