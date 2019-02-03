package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	throttle         = 10
	jobPoolWait      = 1
	numberOfRoutines = 100
)

var (
	objects     = map[bool]map[string]*Result{true: map[string]*Result{}, false: map[string]*Result{}}
	tSet        = map[int][]Job{}
	throttler   = time.Tick(throttle * time.Millisecond)
	mutex       = &sync.Mutex{}
	jobMutex    = &sync.Mutex{}
	startObject Object
)

type Object interface {
	GetType() string
	GetName() string
	GetUrl() string
	GetRole() string
}

type Person struct {
	Name string
	Url  string
	Role string
}

func (p *Person) GetName() string {
	return p.Name
}

func (p *Person) GetUrl() string {
	return p.Url
}

func (p *Person) GetRole() string {
	return p.Role
}

func (p *Person) GetType() string {
	return "p"
}

type Movie struct {
	Name string
	Url  string
	Role string
}

func (m *Movie) GetName() string {
	return m.Name
}

func (m *Movie) GetUrl() string {
	return m.Url
}

func (m *Movie) GetRole() string {
	return m.Role
}

func (m *Movie) GetType() string {
	return "m"
}

type Response struct {
	Type   string
	Name   string
	Crew   []*Person
	Cast   []*Person
	Movies []*Movie
}

type Result struct {
	Parent *Result
	Object Object
}

type Output struct {
	Movie string
	Role1 string
	Role2 string
	Name1 string
	Name2 string
}

type Job struct {
	Id     bool
	Object Object
	Result *Result
	Level  int
}

// ********************************* Queue Implementation *********************************

func add(job Job) {
	jobMutex.Lock()
	if _, ok := tSet[job.Level]; !ok {
		tSet[job.Level] = make([]Job, 1)
	}
	tSet[job.Level] = append(tSet[job.Level], job)
	jobMutex.Unlock()
}

func pop() Job {
	jobMutex.Lock()
	var r Job
	for level := 0; tSet[level] != nil; level++ {
		if set := tSet[level]; len(set) > 0 {
			r = set[0]
			tSet[level] = set[1:]
			break
		}
	}
	jobMutex.Unlock()
	return r
}

func popper(wg *sync.WaitGroup, jobChan chan Job, doneChan <-chan struct{}) {
PL:
	for true {
		select {
		case <-doneChan:
			break PL
		default:
			job := pop()
			if job.Result != nil {
				select {
				case jobChan <- job:
				}
			} else {
				<-time.Tick(jobPoolWait * time.Millisecond)
			}
		}
	}
	wg.Done()
}

// ***************************************** Main Job Handler **************************************************

func worker(wg *sync.WaitGroup, jobChan <-chan Job, doneChan chan struct{}) {
L:
	for true {
		select {
		case <-doneChan:
			break L
		case job, ok := <-jobChan:
			if ok {
			IL:
				for _, o := range getObjects(job.Object) {
					select {
					case <-doneChan:
						break IL
					default:
						res := &Result{Object: o, Parent: job.Result}
						mutex.Lock()
						if _, ok := objects[job.Id][o.GetUrl()]; !ok {
							objects[job.Id][o.GetUrl()] = res
							if otherRes, ok := objects[!job.Id][o.GetUrl()]; ok {
								close(doneChan)
								processResult(res, otherRes)
								break IL
							} else {
								add(Job{Id: job.Id, Object: o, Result: res, Level: job.Level + 1})
							}
						}
						mutex.Unlock()
					}
				}
			}
		}
	}
	wg.Done()
}

// ***************************************** OutPut Formatting **************************************************

func formatResult(res1, res2 *Result, qres []*Output) []*Output {
	if res1.Object.GetType() == "m" {
		f := qres[0]
		f.Movie = res1.Object.GetName()
		f.Role1 = res1.Object.GetRole()
		l := qres[len(qres)-1]
		l.Movie = res2.Object.GetName()
		l.Role2 = res2.Object.GetRole()
	} else {
		if res1.Parent == nil {
			f := qres[0]
			f.Name1 = res1.Object.GetUrl()
			l := qres[len(qres)-1]
			l.Name2 = res2.Object.GetUrl()
			return qres
		} else {
			f := qres[0]
			f.Name1 = res1.Object.GetName()
			n := &Output{Name2: res1.Object.GetName(), Role1: res2.Object.GetRole()}
			qres = append([]*Output{n}, qres...)
			l := qres[len(qres)-1]
			l.Name2 = res2.Object.GetName()
			n = &Output{Name1: res2.Object.GetName(), Role1: res2.Object.GetRole()}
			qres = append(qres, n)
		}
	}
	return formatResult(res1.Parent, res2.Parent, qres)
}

func processResult(res1, res2 *Result) {
	qres := []*Output{}
	if res1.Object.GetType() == "m" {
		qres = append(qres, &Output{Movie: res1.Object.GetName(), Role1: res1.Object.GetRole(), Role2: res2.Object.GetRole()})
	} else {
		qres = append(qres, &Output{Name2: res1.Object.GetName(), Role2: res1.Object.GetRole()})
		qres = append(qres, &Output{Name1: res2.Object.GetName(), Role1: res2.Object.GetRole()})
	}
	qres = formatResult(res1.Parent, res2.Parent, qres)
	printResult(qres)
}

func printResult(res []*Output) {
	fmt.Println("Degrees of Separation:  ", len(res))
	var f func(int) *Output
	if res[0].Name1 == startObject.GetUrl() {
		f = func(i int) *Output {
			return res[i]
		}
	} else {
		f = func(i int) *Output {
			return res[len(res)-1-i]
		}
	}
	for i := 0; i < len(res); i++ {
		out := f(i)
		fmt.Println(i+1, ". Movie : ", out.Movie)
		fmt.Println("   ", out.Role1, ": ", out.Name1)
		fmt.Println("   ", out.Role2, ": ", out.Name2)
		fmt.Println("")
	}
	fmt.Println("Closing all goroutines.....")
}

// ***************************************** HTTP Handlers **************************************************

func getObjects(left Object) []Object {
	if left.GetType() == "p" {
		return getMovies(left)
	} else {
		return getPersons(left)
	}
}

func getMovies(p Object) []Object {
	res := get(p.GetUrl())
	ress := make([]Object, 0)
	for _, m := range res.Movies {
		ress = append(ress, Object(m))
	}
	return ress
}

func getPersons(m Object) []Object {
	res := get(m.GetUrl())
	ress := make([]Object, 0)
	for _, p := range res.Crew {
		ress = append(ress, Object(p))
	}
	for _, p := range res.Cast {
		ress = append(ress, Object(p))
	}
	return ress
}

func get(u string) *Response {
	<-throttler
	resp, _ := http.Get(fmt.Sprintf("http://data.moviebuff.com/%s", u))
	res := &Response{}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(body, res)
	return res
}

// **************************************** goroutine Pool Manaagement ********************************

func createWorkerPool(noOfWorkers int) {
	var wg sync.WaitGroup
	var jobChan = make(chan Job, noOfWorkers*2)
	var doneChan = make(chan struct{})
	wg.Add(1)
	go popper(&wg, jobChan, doneChan)
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg, jobChan, doneChan)
	}
	wg.Wait()
}

// ***************************************** Input Verify **************************************************

func verifyInput(inp []string) error {
	if len(inp) != 2 {
		fmt.Println("Did't get enough arguments. Please run the program by calling in the following way:")
		fmt.Println("go run degrees.go amitabh-bachchan robert-de-niro")
		return errors.New("Argument Error")
	}
	if inp[0] == inp[1] {
		fmt.Println("Degrees of Separation: 0")
		return errors.New("Given 2 strings are same")
	}
	return nil
}

// ****************************************** Main *********************************************************

func main() {
	argsWithoutProg := os.Args[1:]
	err := verifyInput(argsWithoutProg)
	if err != nil {
		return
	}
	p1 := &Person{Url: argsWithoutProg[0]}
	p2 := &Person{Url: argsWithoutProg[1]}
	startObject = p1
	add(Job{Object: p1, Id: true, Result: &Result{Object: p1}, Level: 0})
	add(Job{Object: p2, Id: false, Result: &Result{Object: p2}, Level: 0})
	createWorkerPool(numberOfRoutines)
}
