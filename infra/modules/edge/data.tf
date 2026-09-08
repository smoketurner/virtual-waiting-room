# No data sources are needed for the CloudFront distribution: the two cache
# policies for uncached behaviours use the AWS-managed CachingDisabled policy by
# its well-known ID (locals.tf), and the polled policies are created inline.
#
# Data sources / managed rule-group lookups return when the WAFv2 web ACL and
# the standby alarm are added (both us-east-1).
