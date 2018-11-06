#Event scheduler

Event scheduler accepts pairs of LocalDateTime and Callable.
Scheduler executes callable for each pair when particular local date time occurs.
Or runs callable in place if date time is already passed.
Taks are executed in order of local date time or ion order of apperance if date time are equal.
Events might come from different threads and in any order.
