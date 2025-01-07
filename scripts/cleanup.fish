#!/usr/bin/env fish

if test (count $argv) -lt 1
	echo "Usage: cleanup <namespace>"
	return 1
end

set namespace $argv[1]
set resources "authpolicy" "ratelimitpolicy" "gateway" "httproute"

for resource in $resources
	# Attempt to delete resources
	echo "Deleting all $resource resources in the $namespace namespace..."
	kubectl -n $namespace delete $resource --all

	# Check the exit status
	if test $status -eq 0
		echo "Successfully deleted all $resource resources in the $namespace namespace."
	else
		echo "Failed to delete resources. Please check for errors."
	end
	echo
end

