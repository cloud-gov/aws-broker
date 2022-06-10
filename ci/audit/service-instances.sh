#!/bin/bash -exu

function main() {
  cf api "${CF_API_URL}"
  (set +x; cf auth "${CF_USERNAME}" "${CF_PASSWORD}")

  cg-scripts/audit/cf-service-instances-by-service-offering.sh ${CF_SERVICE} >> email-out/${CF_SERVICE}.txt
}

main