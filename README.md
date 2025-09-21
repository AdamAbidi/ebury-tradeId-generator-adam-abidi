# Identity Service

FX Labs has many different systems generating currency trades.
So that our clients can easily look up individual trades, we
want to assign each trade its unique 7-character alphanumeric
human-readable ID.

Example: B762F00

An API will use this package to create new unique IDs on demand.

## Setup

You will need a Python environment with the package requirements
installed.

To set this up using `virtualenv`, run:

```
$ virtualenv -p python3 venv
$ source venv/bin/activate
$ pip3 install -r requirements.txt
```

You can run the tests with:

```
$ python -m pytest
```

## Task

This repo contains tests for code to generate the IDs using the functions provided inside the `identity/generation.py` file as an entry point. Your challenge is completing the code, not limited to that specific script or functions.

You can merge one branch at a time into the `python-test` branch, run the test suite, improve your solution and commit the changes before merging the next one. 

The branches are:

1. `origin/python-bulk-generation` - adds tests for a bulk generation function
to generate many IDs at once, and improves the uniqueness and formatting
tests.

1. `origin/python-concurrency` - tests that the code can handle many concurrent
requests in a multithreaded environment.

1. `origin/python-persistence-and-fault-tolerance` - tests that the code can
recover from crashes and restarts without duplicating IDs.

1. `origin/python-performance` - tests the code performance.

**Important:** the tests above are there to guide you through the requirements and are not exhaustive - think about how you would solve this problem in a real-life scenario.

Finally, once you have finished, please create a git bundle to send back to
us with this command:

```
$ git bundle create repo.bundle --all
```

Good luck!

## FAQ

**1. Can I use external libraries or dependencies?** Yes, you can! However, ensure that you can help us quickly set up the environment to run your solution.

**2. Can I add more tests to the test suite?** Actually, you shouldn't rely only on the tests we provide but do look at them to understand what might be important. Feel comfortable adding/improving the tests that are there.

**3. I quickly solved the problem in a few minutes; what is happening?** The chances you are not carefully looking at the problem are high, even if all the tests pass. Remember to exercise the solution in terms of production readiness, concurrency, performance, and fault tolerance.

**4. How much time do I have to solve the problem?** It may take a few hours to implement your solution, but perhaps spread over a number of days. The faster we receive the solution, the faster we can review it and schedule the next step - it will be perfect not to take more than four days.

**5. Should I use the less possible code to solve the problem?** You should follow the Python language's clean code practices and conventions. Other people will review your code, so the readability of the code will be evaluated by design.

**6. I have multiple solutions. Should I send all of them to you?** We discourage sending various solutions to us because you will be delegating the decision to choose the best one - which is not good in a real-life scenario.
