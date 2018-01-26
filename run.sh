command -v jmeter >/dev/null 2>&1 || { echo >&2 "Could not find jmeter. Please ensure it's in your path"; exit 1; }

jmeter -n -t ./simple-example.jmx "$@"
