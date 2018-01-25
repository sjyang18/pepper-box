command -v jmeter >/dev/null 2>&1 || { echo >&2 "Could not find jmeter. Please ensure it's in your path"; exit 1; }

jmeter -n -Jthreadz=7 -Jiterationz=100 -t ./simple-example.jmx "$@"
