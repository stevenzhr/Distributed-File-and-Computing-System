servers=(
    "orion01"
    "orion02"
    "orion03"
    "orion04"
    "orion05"
    "orion06"
    "orion07"
    "orion08"
    "orion09"
    "orion10"
    "orion11"
    "orion12"
)

for server in ${servers[@]}; do
    echo "${server}"
    ssh "${server}" "pkill -u "$(whoami)" node"
done
