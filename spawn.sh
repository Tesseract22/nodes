#!/bin/bash

SESSION="nodes"

num=${1:-4}
tmux new-session -d -s $SESSION

tmux send-keys -t $SESSION "./zig-out/bin/node 8000" C-m
tmux split-window -v -t $SESSION
sleep 1

for i in $(seq 1 $num); do
    tmux send-keys -t $SESSION "./zig-out/bin/node $((i*10 + 8000))" C-m
    tmux split-window -v -t $SESSION
    tmux select-layout -t $SESSION even-vertical
done

tmux attach-session -t $SESSION
