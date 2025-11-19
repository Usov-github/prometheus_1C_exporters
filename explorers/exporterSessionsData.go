package exporter

import (
	"math"
	"strconv"
	"time"

	"github.com/LazarenkoA/prometheus_1C_exporter/explorers/model"
	"github.com/LazarenkoA/prometheus_1C_exporter/settings"
	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/prometheus/client_golang/prometheus"
)

type SummaryVecInterface interface {
	prometheus.Collector
	WithLabelValues(lvs ...string) prometheus.Observer
	Reset()
}

type sessionsData struct {
	basename            string
	appid               string
	user                string
	memorytotal         int64
	memorycurrent       int64
	readcurrent         int64
	readtotal           int64
	writecurrent        int64
	writetotal          int64
	durationcurrent     int64
	durationcurrentdbms int64
	durationall         int64
	durationalldbms     int64
	cputimecurrent      int64
	cputimetotal        int64
	dbmsbytesall        int64
	callsall            int64
	sessionid           string
	startedAt           int64
}

type ExporterSessionsMemory struct {
	ExporterSessions

	buff           map[string]*sessionsData
	summary        SummaryVecInterface
	startedAtGauge *prometheus.GaugeVec
}

func (exp *ExporterSessionsMemory) Construct(s *settings.Settings) *ExporterSessionsMemory {
	exp.BaseExporter = newBase(exp.GetName())
	exp.logger.Info("Создание объекта")

	prefix := s.GetMetricNamePrefix()
	realSummary := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:        prefix + exp.GetName(),
			Help:        "Показатели сессий из кластера 1С",
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{"ras_host": s.GetRASHostPort()},
		},
		[]string{"host", "base", "user", "id", "datatype", "appid"},
	)

	exp.summary = realSummary

	exp.startedAtGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "session_start_timestamp",
			Help: "Timestamp when the 1C session started",
		},
		[]string{"host", "base", "user", "id", "appid"},
	)

	exp.buff = make(map[string]*sessionsData)
	exp.settings = s
	exp.ExporterCheckSheduleJob.settings = s
	exp.cache = expirable.NewLRU[string, []map[string]string](5, nil, time.Second*5)
	go exp.fillBaseList()
	go exp.collectingMetrics(time.Second * 5)

	return exp
}

func atoi(n string) int64 {
	if v, err := strconv.ParseInt(n, 10, 64); err == nil {
		return v
	}
	return 0
}

func (exp *ExporterSessionsMemory) collectingMetrics(delay time.Duration) {
	layout := "2006-01-02T15:04:05"
	for {
		sessions, _ := exp.getSessions()

		for _, item := range sessions {
			sessionid := item["session-id"]
			var startedAtUnix int64
			if startedAt, err := time.Parse(layout, item["started-at"]); err == nil {
				startedAtUnix = startedAt.Unix()
			}

			exp.mx.Lock()
			v, ok := exp.buff[sessionid]
			if !ok {
				v = &sessionsData{
					basename:            exp.findBaseName(item["infobase"]),
					appid:               item["app-id"],
					user:                item["user-name"],
					memorytotal:         atoi(item["memory-total"]),
					memorycurrent:       atoi(item["memory-current"]),
					readcurrent:         atoi(item["read-current"]),
					readtotal:           atoi(item["read-total"]),
					writecurrent:        atoi(item["write-current"]),
					writetotal:          atoi(item["write-total"]),
					durationcurrent:     atoi(item["duration-current"]),
					durationcurrentdbms: atoi(item["duration-current-dbms"]),
					durationall:         atoi(item["duration-all"]),
					durationalldbms:     atoi(item["duration-all-dbms"]),
					cputimecurrent:      atoi(item["cpu-time-current"]),
					cputimetotal:        atoi(item["cpu-time-total"]),
					dbmsbytesall:        atoi(item["dbms-bytes-all"]),
					callsall:            atoi(item["calls-all"]),
					sessionid:           sessionid,
					startedAt:           startedAtUnix,
				}
				exp.buff[sessionid] = v
			} else {
				v.memorycurrent = int64(math.Max(float64(v.memorycurrent), float64(atoi(item["memory-current"]))))
				v.readcurrent = int64(math.Max(float64(v.readcurrent), float64(atoi(item["read-current"]))))
				v.cputimecurrent = int64(math.Max(float64(v.cputimecurrent), float64(atoi(item["cpu-time-current"]))))
				v.durationcurrentdbms = int64(math.Max(float64(v.durationcurrentdbms), float64(atoi(item["duration-current-dbms"]))))
				v.durationcurrent = int64(math.Max(float64(v.durationcurrent), float64(atoi(item["duration-current"]))))
				v.writecurrent = int64(math.Max(float64(v.writecurrent), float64(atoi(item["write-current"]))))
				v.dbmsbytesall = atoi(item["dbms-bytes-all"])
				v.cputimetotal = atoi(item["cpu-time-total"])
				v.durationalldbms = atoi(item["duration-all-dbms"])
				v.durationall = atoi(item["duration-all"])
				v.writetotal = atoi(item["writetotal"])
				v.readtotal = atoi(item["read-total"])
				v.memorytotal = atoi(item["memory-total"])
				v.callsall = atoi(item["calls-all"])
				v.startedAt = startedAtUnix
			}
			exp.mx.Unlock()
		}

		select {
		case <-time.After(delay):
		case <-exp.ctx.Done():
			return
		}
	}
}

func (exp *ExporterSessionsMemory) getValue() {
	exp.logger.Info("получение данных экспортера")

	exp.mx.Lock()
	defer exp.mx.Unlock()

	exp.summary.Reset()
	for _, v := range exp.buff {
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "memorytotal", v.appid).Observe(float64(v.memorytotal))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "memorycurrent", v.appid).Observe(float64(v.memorycurrent))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "readcurrent", v.appid).Observe(float64(v.readcurrent))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "readtotal", v.appid).Observe(float64(v.readtotal))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "writecurrent", v.appid).Observe(float64(v.writecurrent))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "writetotal", v.appid).Observe(float64(v.writetotal))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "durationcurrent", v.appid).Observe(float64(v.durationcurrent))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "durationcurrentdbms", v.appid).Observe(float64(v.durationcurrentdbms))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "durationall", v.appid).Observe(float64(v.durationall))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "durationalldbms", v.appid).Observe(float64(v.durationalldbms))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "cputimecurrent", v.appid).Observe(float64(v.cputimecurrent))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "cputimetotal", v.appid).Observe(float64(v.cputimetotal))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "dbmsbytesall", v.appid).Observe(float64(v.dbmsbytesall))
		exp.summary.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, "callsall", v.appid).Observe(float64(v.callsall))

		exp.startedAtGauge.WithLabelValues(exp.host, v.basename, v.user, v.sessionid, v.appid).Set(float64(v.startedAt))
	}

	exp.buff = make(map[string]*sessionsData)
}

func (exp *ExporterSessionsMemory) Collect(ch chan<- prometheus.Metric) {
	if exp.isLocked.Load() {
		return
	}

	exp.getValue()
	exp.summary.Collect(ch)
	exp.startedAtGauge.Collect(ch)
}

func (exp *ExporterSessionsMemory) GetName() string {
	return "sessions_data"
}

func (exp *ExporterSessionsMemory) GetType() model.MetricType {
	return model.TypeRAC
}
