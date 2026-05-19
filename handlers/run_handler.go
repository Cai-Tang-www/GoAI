package handlers

import (
	"errors"
	"net/http"

	"GoAI/services"

	"github.com/gin-gonic/gin"
)

func CreateRun(c *gin.Context) {
	var req services.CreateRunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	userID, ok := authUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	run, err := services.CreateRun(c.Request.Context(), userID, req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, services.CreateRunResponse{
		RunID:  run.RunID,
		Status: run.Status,
	})
}

func GetRun(c *gin.Context) {
	userID, ok := authUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	runID := c.Param("run_id")
	run, err := services.GetRunByRunID(c.Request.Context(), userID, runID)
	if err != nil {
		if errors.Is(err, services.ErrRunForbidden()) {
			c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return
		}
		if errors.Is(err, services.ErrRunNotFound()) {
			c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, run)
}

func ListRunSteps(c *gin.Context) {
	userID, ok := authUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	runID := c.Param("run_id")
	steps, err := services.GetRunStepsByRunID(c.Request.Context(), userID, runID)
	if err != nil {
		if errors.Is(err, services.ErrRunForbidden()) {
			c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return
		}
		if errors.Is(err, services.ErrRunNotFound()) {
			c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, steps)
}

func ReplayRun(c *gin.Context) {
	userID, ok := authUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	runID := c.Param("run_id")
	newRun, err := services.ReplayRun(c.Request.Context(), userID, runID)
	if err != nil {
		if errors.Is(err, services.ErrRunForbidden()) {
			c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return
		}
		if errors.Is(err, services.ErrRunNotFound()) {
			c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, services.CreateRunResponse{
		RunID:  newRun.RunID,
		Status: newRun.Status,
	})
}

func authUserID(c *gin.Context) (uint64, bool) {
	rawUserID, exists := c.Get("user_id")
	if !exists {
		return 0, false
	}
	userID, ok := rawUserID.(uint)
	if !ok {
		return 0, false
	}
	return uint64(userID), true
}
