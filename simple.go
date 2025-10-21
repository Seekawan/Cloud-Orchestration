package function
 
import (

	"encoding/json"

	"net/http"

	"strconv"

)
 
type sumResponse struct {

	A   float64 `json:"a"`

	B   float64 `json:"b"`

	Sum float64 `json:"sum"`

}
 
// Sum is an HTTP Cloud Function entry point.

// It expects query parameters ?a=<number>&b=<number>

func Sum(w http.ResponseWriter, r *http.Request) {

	q := r.URL.Query()

	aStr := q.Get("a")

	bStr := q.Get("b")
 
	if aStr == "" || bStr == "" {

		http.Error(w, "missing 'a' or 'b' query parameter", http.StatusBadRequest)

		return

	}
 
	a, errA := strconv.ParseFloat(aStr, 64)

	b, errB := strconv.ParseFloat(bStr, 64)

	if errA != nil || errB != nil {

		http.Error(w, "query parameters 'a' and 'b' must be valid numbers", http.StatusBadRequest)

		return

	}
 
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	json.NewEncoder(w).Encode(sumResponse{A: a, B: b, Sum: a + b})

}

 
