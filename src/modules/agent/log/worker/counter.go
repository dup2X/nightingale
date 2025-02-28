package worker

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/toolkits/pkg/logger"

	"github.com/dup2X/nightingale/src/common/dataobj"
	"github.com/dup2X/nightingale/src/modules/agent/log/strategy"
	"github.com/dup2X/nightingale/src/modules/agent/stra"
)

//从worker往计算部分推的Point
type AnalysPoint struct {
	StrategyID int64
	Value      float64
	Tms        int64
	Tags       map[string]string
}

//统计的实体
type PointCounter struct {
	sync.RWMutex
	Count int64
	Sum   float64
	Max   float64
	Min   float64
}

// 单策略下，单step的统计对象
// 以Sorted的tagkv的字符串来做索引
type PointsCounter struct {
	sync.RWMutex
	TagstringMap map[string]*PointCounter
}

// 单策略下的对象, 以step为索引, 索引每一个Step的统计
// 单step统计, 推送完则删
type StrategyCounter struct {
	sync.RWMutex
	Strategy  *stra.Strategy           //Strategy结构体扔这里，以备不时之需
	TmsPoints map[int64]*PointsCounter //按照时间戳分类的分别的counter
}

type baseCollector struct {
	base      *prometheus.CounterVec
	baseGauge *prometheus.GaugeVec
}

// 全局counter对象, 以key为索引，索引每个策略的统计
// key : Strategy ID
type GlobalCounter struct {
	sync.RWMutex
	StrategyCounts map[int64]*StrategyCounter
	rpcCounter     *prometheus.CounterVec
	rpcErrCounter  *prometheus.CounterVec
	rpcLentency    *prometheus.GaugeVec
	rpcTP          *prometheus.HistogramVec
	common         map[int64]*baseCollector
}

var GlobalCount *GlobalCounter

var (
	gPusher *push.Pusher
	gReg    *prometheus.Registry
)

func init() {
	GlobalCount = new(GlobalCounter)
	GlobalCount.StrategyCounts = make(map[int64]*StrategyCounter)
	GlobalCount.common = make(map[int64]*baseCollector)
	gPusher = push.New("http://127.0.0.1:9091", "agent_xx")
	gReg = prometheus.NewRegistry()
	gPusher.Gatherer(gReg)
}

// 提供给Worker用来Push计算后的信息
// 需保证线程安全
func PushToCount(Point *AnalysPoint) error {
	stCount, err := GlobalCount.GetStrategyCountByID(Point.StrategyID)

	// 更新strategyCounts
	if err != nil {
		strategy, err := strategy.GetByID(Point.StrategyID)
		if err != nil {
			logger.Errorf("GetByID ERROR when count:[%v]", err)
			return err
		}
		GlobalCount.AddStrategyCount(strategy)

		stCount, err = GlobalCount.GetStrategyCountByID(Point.StrategyID)
		// 还拿不到，就出错返回吧
		if err != nil {
			logger.Errorf("Get strategyCount Failed after addition: %v", err)
			return err
		}
	}
	// 处理RPC相关的指标
	var vs []string
	for _, k := range stCount.Strategy.SortedTagKey {
		vs = append(vs, Point.Tags[k])
	}
	switch stCount.Strategy.MeasurementType {
	case "rpc":
		var (
			code string
		)
		code = Point.Tags["code"]
		GlobalCount.rpcCounter.WithLabelValues(vs...).Inc()
		if code != "0" {
			GlobalCount.rpcErrCounter.WithLabelValues(vs...).Inc()
		}
		GlobalCount.rpcTP.WithLabelValues(vs...).Observe(Point.Value)
		GlobalCount.rpcLentency.WithLabelValues(vs...).Set(Point.Value)
	case "cnt":
		GlobalCount.common[Point.StrategyID].base.WithLabelValues(vs...).Inc()
	case "avg":
		GlobalCount.common[Point.StrategyID].baseGauge.WithLabelValues(vs...).Inc()
	}

	// 拿到stCount，更新StepCounts
	stepTms := AlignStepTms(stCount.Strategy.Interval, Point.Tms)
	tmsCount, err := stCount.GetByTms(stepTms)
	if err != nil {
		err := stCount.AddTms(stepTms)
		if err != nil {
			logger.Errorf("Add tms to strategy error: %v", err)
			return err
		}

		tmsCount, err = stCount.GetByTms(stepTms)
		// 还拿不到，就出错返回吧
		if err != nil {
			logger.Errorf("Get tmsCount Failed By Twice Add: %v", err)
			return err
		}
	}

	//拿到tmsCount, 更新TagstringMap
	tagstring := dataobj.SortedTags(Point.Tags)
	return tmsCount.Update(tagstring, Point.Value)
}

//时间戳向前对齐
func AlignStepTms(step, tms int64) int64 {
	if step <= 0 {
		return tms
	}
	newTms := tms - (tms % step)
	return newTms
}

func (pc *PointCounter) UpdateCnt() {
	atomic.AddInt64(&pc.Count, 1)
}

func (pc *PointCounter) UpdateSum(value float64) {
	addFloat64(&pc.Sum, value)
}

func (pc *PointCounter) UpdateMaxMin(value float64) {
	// 这里要用到结构体的小锁
	// sum和cnt可以不用锁，但是最大最小没办法做到原子操作
	// 只能引入锁
	pc.RLock()
	if math.IsNaN(pc.Max) || value > pc.Max {
		pc.RUnlock()
		pc.Lock()
		if math.IsNaN(pc.Max) || value > pc.Max {
			pc.Max = value
		}
		pc.Unlock()
	} else {
		pc.RUnlock()
	}

	pc.RLock()
	if math.IsNaN(pc.Min) || value < pc.Min {
		pc.RUnlock()
		pc.Lock()
		if math.IsNaN(pc.Min) || value < pc.Min {
			pc.Min = value
		}
		pc.Unlock()
	} else {
		pc.RUnlock()
	}
}

func (psc *PointsCounter) GetBytagstring(tagstring string) (*PointCounter, error) {
	psc.RLock()
	point, ok := psc.TagstringMap[tagstring]
	psc.RUnlock()

	if !ok {
		return nil, fmt.Errorf("tagstring [%s] not exists", tagstring)
	}
	return point, nil
}

func (psc *PointsCounter) Update(tagstring string, value float64) error {
	pointCount, err := psc.GetBytagstring(tagstring)
	if err != nil {
		tmp := new(PointCounter)
		tmp.Count = 0
		tmp.Sum = 0

		if value == -1 {
			tmp.Sum = math.NaN() //补零逻辑，不处理Sum
		}
		tmp.Max = math.NaN()
		tmp.Min = math.NaN()

		var has bool
		psc.Lock()
		pointCount, has = psc.TagstringMap[tagstring]
		if !has {
			psc.TagstringMap[tagstring] = tmp
			pointCount = tmp
		}
		psc.Unlock()
	}

	pointCount.Lock()

	if value != -1 { //value=-1,是补零逻辑，不做特殊处理
		pointCount.Sum = pointCount.Sum + value
		if math.IsNaN(pointCount.Max) || value > pointCount.Max {
			pointCount.Max = value
		}
		if math.IsNaN(pointCount.Min) || value < pointCount.Min {
			pointCount.Min = value
		}

		pointCount.Count = pointCount.Count + 1
	}

	pointCount.Unlock()

	return nil
}

func addFloat64(val *float64, delta float64) (new float64) {
	for {
		old := *val
		new = old + delta
		if atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(val)),
			math.Float64bits(old),
			math.Float64bits(new),
		) {
			break
		}
	}
	return
}

func (sc *StrategyCounter) GetTmsList() []int64 {
	var tmsList []int64
	sc.RLock()
	for tms := range sc.TmsPoints {
		tmsList = append(tmsList, tms)
	}
	sc.RUnlock()
	return tmsList
}

func (sc *StrategyCounter) DeleteTms(tms int64) {
	sc.Lock()
	delete(sc.TmsPoints, tms)
	sc.Unlock()
}

func (sc *StrategyCounter) GetByTms(tms int64) (*PointsCounter, error) {
	sc.RLock()
	psCount, ok := sc.TmsPoints[tms]
	if !ok {
		sc.RUnlock()
		return nil, fmt.Errorf("no this tms:%v", tms)
	}
	sc.RUnlock()
	return psCount, nil
}

func (sc *StrategyCounter) AddTms(tms int64) error {
	sc.Lock()
	_, ok := sc.TmsPoints[tms]
	if !ok {
		tmp := new(PointsCounter)
		tmp.TagstringMap = make(map[string]*PointCounter)
		sc.TmsPoints[tms] = tmp
	}
	sc.Unlock()
	return nil
}

// 只做更新和删除，添加 由数据驱动
func (gc *GlobalCounter) UpdateByStrategy(globalStras map[int64]*stra.Strategy) {
	var delCount, upCount int
	// 先以count的ID为准，更新count
	// 若ID没有了, 那就删掉
	for _, id := range gc.GetIDs() {
		gc.RLock()
		sCount, ok := gc.StrategyCounts[id]
		gc.RUnlock()

		if !ok || sCount.Strategy == nil {
			//证明此策略无效，或已被删除
			//删一下
			delCount = delCount + 1
			gc.deleteByID(id)
			continue
		}

		newStrategy := globalStras[id]

		// 一个是sCount.Strategy, 一个是newStrategy
		// 开始比较
		if !countEqual(newStrategy, sCount.Strategy) {
			//需要清空缓存
			upCount = upCount + 1
			logger.Infof("strategy [%d] changed, clean data", id)
			gc.cleanStrategyData(id)
			sCount.Strategy = newStrategy
		} else {
			gc.upStrategy(newStrategy)
		}
	}
	logger.Infof("Update global count done, [del:%d][update:%d]", delCount, upCount)
}

func (gc *GlobalCounter) AddStrategyCount(st *stra.Strategy) {
	gc.Lock()
	if _, ok := gc.StrategyCounts[st.ID]; !ok {
		tmp := new(StrategyCounter)
		tmp.Strategy = st
		tmp.TmsPoints = make(map[int64]*PointsCounter)
		var err error
		if gc.rpcCounter == nil && st.MeasurementType == "rpc" {
			gc.rpcCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: st.ServiceName, Subsystem: strings.ReplaceAll(st.Name, ".", "_"), Name: "rpc_count",
				Help: "rpc counter",
			}, st.SortedTagKey)
			err = gReg.Register(gc.rpcCounter)
			if err != nil {
				logger.Warningf("register failed,err:%v", err)
			}
			gc.rpcErrCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: st.ServiceName, Subsystem: strings.ReplaceAll(st.Name, ".", "_"), Name: "rpc_err_count",
				Help: "rpc err counter",
			}, st.SortedTagKey)
			err = gReg.Register(gc.rpcErrCounter)
			if err != nil {
				logger.Warningf("register failed,err:%v", err)
			}
			gc.rpcLentency = prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: st.ServiceName, Subsystem: strings.ReplaceAll(st.Name, ".", "_"), Name: "rpc_lantency",
				Help: "rpc lentency",
			}, st.SortedTagKey)
			err = gReg.Register(gc.rpcLentency)
			if err != nil {
				logger.Warningf("register failed,err:%v", err)
			}
			gc.rpcTP = prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: st.ServiceName, Subsystem: strings.ReplaceAll(st.Name, ".", "_"), Name: "rpc_tp",
				Help:    "rpc tp50 tp99..",
				Buckets: []float64{50, 100, 200, 500, 1000, 2000, 5000}},
				st.SortedTagKey,
			)
			err = gReg.Register(gc.rpcTP)
			if err != nil {
				logger.Warningf("register failed,err:%v", err)
			}
		}
		if st.MeasurementType != "rpc" {
			if _, ok := gc.common[st.ID]; !ok {
				gc.common[st.ID] = &baseCollector{
					base: prometheus.NewCounterVec(prometheus.CounterOpts{
						Namespace: st.ServiceName, Name: strings.ReplaceAll(st.Name, ".", "_"),
						Help: "base counter..",
					}, st.SortedTagKey),
					baseGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
						Namespace: st.ServiceName, Name: strings.ReplaceAll(st.Name, ".", "_"),
						Help: "base gauge counter",
					}, st.SortedTagKey),
				}
				err = gReg.Register(gc.common[st.ID].base)
				if err != nil {
					logger.Warningf("register failed,err:%v", err)
				}
				err = gReg.Register(gc.common[st.ID].baseGauge)
				if err != nil {
					logger.Warningf("register failed,err:%v", err)
				}
				logger.Infof("register %v for %d\n", gc.common[st.ID].base, st.ID)
			}
		}
		gc.StrategyCounts[st.ID] = tmp
	}
	gc.Unlock()
}

func (gc *GlobalCounter) upStrategy(st *stra.Strategy) {
	gc.Lock()
	if _, ok := gc.StrategyCounts[st.ID]; ok {
		gc.StrategyCounts[st.ID].Strategy = st
	}
	gc.Unlock()
}

func (gc *GlobalCounter) GetStrategyCountByID(id int64) (*StrategyCounter, error) {
	gc.RLock()
	stCount, ok := gc.StrategyCounts[id]
	if !ok {
		gc.RUnlock()
		return nil, fmt.Errorf("No this ID")
	}
	gc.RUnlock()
	return stCount, nil
}

func (gc *GlobalCounter) GetIDs() []int64 {
	gc.RLock()
	rList := make([]int64, 0)
	for k := range gc.StrategyCounts {
		rList = append(rList, k)
	}
	gc.RUnlock()
	return rList
}

func (gc *GlobalCounter) deleteByID(id int64) {
	gc.Lock()
	delete(gc.StrategyCounts, id)
	gc.Unlock()
}

func (gc *GlobalCounter) cleanStrategyData(id int64) {
	gc.RLock()
	sCount, ok := gc.StrategyCounts[id]
	gc.RUnlock()
	if !ok || sCount == nil {
		return
	}
	sCount.TmsPoints = make(map[int64]*PointsCounter)
}

func prometheusPush() error {
	return gPusher.Push()
}

// countEqual意味着不会对统计的结构产生影响
func countEqual(A *stra.Strategy, B *stra.Strategy) bool {
	if A == nil || B == nil {
		return false
	}
	if A.Pattern == B.Pattern && A.Interval == B.Interval && A.Func == B.Func && reflect.DeepEqual(A.Tags, B.Tags) {
		return true
	}
	return false
}
