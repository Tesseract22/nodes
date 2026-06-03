#!/bin/bash

SESSION="quad-split"

tmux new-session -d -s $SESSION

tmux send-keys -t $SESSION "./zig-out/bin/node 0" C-m
tmux split-window -v -t $SESSION
sleep 1

for i in {1..3}; do
    tmux send-keys -t $SESSION "./zig-out/bin/node $i" C-m
    tmux split-window -v -t $SESSION
done

tmux select-layout -t $SESSION even-vertical
tmux attach-session -t $SESSION
