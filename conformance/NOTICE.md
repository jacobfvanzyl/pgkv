# Where the scenarios come from

`scenarios.jsonl` holds 372 command sequences derived from the test suite of
[Valkey](https://github.com/valkey-io/valkey) at commit `1c9b6a38`: `tests/unit/expire.tcl` (34), `tests/unit/keyspace.tcl` (16), `tests/unit/other.tcl` (3), `tests/unit/type/hash.tcl` (28), `tests/unit/type/incr.tcl` (35), `tests/unit/type/list-3.tcl` (2), `tests/unit/type/list.tcl` (70), `tests/unit/type/set.tcl` (38), `tests/unit/type/string.tcl` (56), `tests/unit/type/zset.tcl` (90).
Each line names the test and the file and line it was taken from. They are the commands
those tests send, without the Tcl around them. The expected results are not copied: the
harness asks a live server instead.

No scenario is taken from the Redis repository, whose tests are under different terms.

Valkey's licence (`COPYING` at that commit):

```
BSD 3-Clause License

Copyright (c) 2024-present, Valkey contributors
Copyright (c) 2006-2020, Redis Ltd.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```
