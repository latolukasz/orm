package orm

import (
	"fmt"
	"strconv"
	"strings"
)

type HealthCheckStep struct {
	Name        string
	Description string
}

type HealthCheckError struct {
	HealthCheckStep
	Message string
}

func (e *Engine) HealthCheck() (errors []HealthCheckError, warnings []HealthCheckError, valid []HealthCheckStep) {
	for pool, def := range e.registry.sqlClients {
		db := e.GetMysql(pool)
		step := HealthCheckStep{
			Name:        "query MySQL " + strings.ToUpper(pool),
			Description: "being able to query MySQL " + def.databaseName + " database",
		}
		err := healthCheck(func() {
			arg := 0
			db.QueryRow(NewWhere("SELECT 3"), &arg)
		})
		if err != nil {
			errors = append(errors, HealthCheckError{step, err.Error()})
		} else {
			valid = append(valid, step)
		}
	}
	usedAddresses := make(map[string]bool)
	for pool, def := range e.registry.redisServers {
		_, has := usedAddresses[def.GetAddress()]
		if has {
			continue
		}
		usedAddresses[def.GetAddress()] = true
		r := e.GetRedis(pool)
		step := HealthCheckStep{
			Name:        "query Redis " + strings.ToUpper(pool),
			Description: "being able to query Redis " + def.GetAddress(),
		}
		err := healthCheck(func() {
			r.Set("_orm_health_check", "ok", 10)
		})
		if err != nil {
			errors = append(errors, HealthCheckError{step, err.Error()})
		} else {
			valid = append(valid, step)

			step := HealthCheckStep{
				Name:        "memory usage in Redis " + strings.ToUpper(pool),
				Description: "checking % of used memory in " + def.GetAddress(),
			}
			err := healthCheck(func() {
				info := r.Info("memory")
				lines := strings.Split(info, "\r\n")
				maxMemory := 0
				usedMemory := 0
				for _, line := range lines {
					if line == "" || strings.HasPrefix(line, "#") {
						continue
					}
					row := strings.Split(line, ":")
					val := ""
					if len(row) > 1 {
						val = row[1]
					}
					if row[0] == "maxmemory" {
						maxMemory, _ = strconv.Atoi(val)
					} else if row[0] == "used_memory" {
						usedMemory, _ = strconv.Atoi(val)
					}
				}
				if maxMemory == 0 {
					warnings = append(warnings, HealthCheckError{step, "maxmemory is not defined or zero"})
				} else {
					percentage := int(float64(usedMemory) / float64(maxMemory) * 100)
					if percentage >= 90 {
						errors = append(errors, HealthCheckError{step, fmt.Sprintf("memory usage is very high: %d", percentage) + "%"})
					} else if percentage >= 70 {
						warnings = append(warnings, HealthCheckError{step, fmt.Sprintf("memory usage is high: %d", percentage) + "%"})
					} else {
						valid = append(valid, step)
					}
				}
			})
			if err != nil {
				errors = append(errors, HealthCheckError{step, err.Error()})
			}
		}
	}
	return errors, warnings, valid
}

func healthCheck(f func()) error {
	var err error
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				err = rec.(error)
			}
		}()
		f()
	}()
	return err
}
