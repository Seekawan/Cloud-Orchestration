package main

import (
"context"
"encoding/json"
"fmt"
"net/http"
"os"
"time"

"cloud.google.com/go/storage"
"google.golang.org/api/option"
)

type State struct {
Counter   int                    json:"counter"
Description string                 json:"description"
Metadata  map[string]interface{} json:"metadata"
NextActor string                 json:"next_actor"
}

type Lock struct {
Owner     string json:"owner"
ExpiresAt int64  json:"expires_at"
}

func main() {
http.HandleFunc("/increment", handleIncrement)
http.ListenAndServe(":8080", nil)
}

func handleIncrement(w http.ResponseWriter, r *http.Request) {
ctx := r.Context()
start := time.Now()

bucket := os.Getenv("BUCKET_NAME") // Variable name
if bucket == "" {
httpError(w, "BUCKET_NAME not set", http.StatusInternalServerError)
return
}
stateObj := os.Getenv("STATE_OBJECT")
if stateObj == "" {
stateObj = "switcher.json"
}
lockObj := os.Getenv("LOCK_OBJECT")
if lockObj == "" {
lockObj = "increment.lock"
}

client, err := storage.NewClient(ctx, option.WithoutAuthentication())
if err != nil {
httpError(w, "storage.NewClient: "+err.Error(), http.StatusInternalServerError)
return
}
defer client.Close()

owner := fmt.Sprintf("faas-%d", time.Now().UnixNano())
acquired := false
for i := 0; i < 5; i++ {
if acquireLock(ctx, client, bucket, lockObj, owner) {
acquired = true
break
}
time.Sleep(100 * time.Millisecond)
}
if !acquired {
httpError(w, "could not acquire lock", http.StatusConflict)
return
}
defer releaseLock(ctx, client, bucket, lockObj)

st, err := readState(ctx, client, bucket, stateObj)
if err != nil {
httpError(w, "readState: "+err.Error(), http.StatusInternalServerError)
return
}

// Increment
st.Counter++
if st.Metadata == nil {
st.Metadata = map[string]interface{}{}
}
st.Metadata["last_updated"] = time.Now().UTC().Format(time.RFC3339)
st.Metadata["last_source"] = "faas"
st.Description = "Increment by Cloud Function"

// Flip next actor
if st.NextActor == "vm" {
st.NextActor = "faas"
} else {
st.NextActor = "vm"
}

if err := writeState(ctx, client, bucket, stateObj, st); err != nil {
httpError(w, "writeState: "+err.Error(), http.StatusInternalServerError)
return
}

latency := time.Since(start)
w.Header().Set("Content-Type", "application/json")
w.Write([]byte(fmt.Sprintf({"status":"ok","latency_ms":%d,"counter":%d,"next_actor":"%s"}+"\n", latency.Milliseconds(), st.Counter, st.NextActor)))
}

func acquireLock(ctx context.Context, client *storage.Client, bucket, lockObj, owner string) bool {
obj := client.Bucket(bucket).Object(lockObj)

// Lock with DoesNotExist condition
wc := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
lock := Lock{Owner: owner, ExpiresAt: time.Now().Add(30 * time.Second).Unix()}
if err := json.NewEncoder(wc).Encode(lock); err != nil {
wc.Close()
return false
}
if err := wc.Close(); err != nil {
return false
}
return true
}

func releaseLock(ctx context.Context, client *storage.Client, bucket, lockObj string) {
obj := client.Bucket(bucket).Object(lockObj)
_ = obj.Delete(ctx)
}

func readState(ctx context.Context, client *storage.Client, bucket, stateObj string) (*State, error) {
obj := client.Bucket(bucket).Object(stateObj)
r, err := obj.NewReader(ctx)
if err != nil {
return nil, err
}
defer r.Close()
var st State
if err := json.NewDecoder(r).Decode(&st); err != nil {
return nil, err
}
return &st, nil
}

func writeState(ctx context.Context, client *storage.Client, bucket, stateObj string, st *State) error {
obj := client.Bucket(bucket).Object(stateObj)
w := obj.NewWriter(ctx)
w.ContentType = "application/json"
if err := json.NewEncoder(w).Encode(st); err != nil {
w.Close()
return err
}
return w.Close()
}

func httpError(w http.ResponseWriter, msg string, code int) {
w.WriteHeader(code)
w.Header().Set("Content-Type", "application/json")
w.Write([]byte({"error":" + msg + "} + "\n"))
}
