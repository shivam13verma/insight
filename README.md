# insight

This is the solution to the problem set: https://github.com/InsightDataScience/digital-wallet.

Run as follows:
- Clone repo locally
- For unit tests, `cd insight_testsuite` `bash run_tests.sh`. Should see 3 tests passed.
- The folder `paymo_input` consists of the first 1000 batch and stream inputs. This can be used as a test, or the files replaced with the originals.
- Run code using `bash run.sh` from repo directory.


The antifraud.py source code contains 6 kinds of features:
- Features 1 to 3: As described in original problem.
- Feature 4: Generalization of feature 3 to both users being within K degrees of each other within transaction graph.
- Feature 5: Extension of feature 3, also considering whether last transaction was "a long time ago" (in which case it is unverified).
- Feature 6: Extension of feature 3, also checking if transaction amount exceeds a certain limit, such as previous maximum transaction  (which will vary depending on user) multiplied by a factor.
