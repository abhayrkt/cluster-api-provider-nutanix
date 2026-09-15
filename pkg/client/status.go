/*
Copyright 2022 Nutanix

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	nutanixClientV3 "github.com/nutanix-cloud-native/prism-go-client/v3"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	pollingInterval = time.Second * 2
	statusSucceeded = "SUCCEEDED"
)

// WaitForTaskToSucceed will poll every 2 seconds for the task with uuid to have status of "SUCCEEDED".
// The polling will stop if the ctx is cancelled, it's used for HTTP requests in the client and to control the polling.
// WaitForTaskToSucceed will exit immediately on an error getting the task.
func WaitForTaskToSucceed(ctx context.Context, conn *nutanixClientV3.Client, uuid string) error {
	return wait.PollUntilContextCancel(ctx, pollingInterval, true, func(ctx context.Context) (done bool, err error) {
		status, getErr := GetTaskStatus(ctx, conn, uuid)
		return status == statusSucceeded, getErr
	})
}

func GetTaskStatus(ctx context.Context, client *nutanixClientV3.Client, uuid string) (string, error) {
	log := ctrl.LoggerFrom(ctx)
	log.V(1).Info(fmt.Sprintf("Getting task with UUID %s", uuid))
	v, err := client.V3.GetTask(ctx, uuid)
	if err != nil {
		log.Error(err, fmt.Sprintf("error occurred while waiting for task with UUID %s", uuid))
		return "", err
	}

	if *v.Status == "INVALID_UUID" || *v.Status == "FAILED" {
		errMsg := fmt.Sprintf("error_detail: %s, progress_message: %s", ptr.Deref(v.ErrorDetail, ""), ptr.Deref(v.ProgressMessage, ""))
		subtaskErrs := collectFailedV3SubtaskErrors(ctx, client, v, map[string]struct{}{uuid: {}})
		if len(subtaskErrs) > 0 {
			log.Error(errors.New(errMsg), "Prism parent task failed; including failed subtask errors",
				"taskUUID", uuid,
				"subtaskErrors", subtaskErrs,
			)
			errMsg = fmt.Sprintf("%s; failed_subtasks: %s", errMsg, strings.Join(subtaskErrs, "; "))
		}
		return *v.Status, errors.New(errMsg)
	}
	taskStatus := *v.Status
	log.V(1).Info(fmt.Sprintf("Status for task with UUID %s: %s", uuid, taskStatus))
	return taskStatus, nil
}

func collectFailedV3SubtaskErrors(ctx context.Context, client *nutanixClientV3.Client, parent *nutanixClientV3.TasksResponse, visited map[string]struct{}) []string {
	if parent == nil || len(parent.SubtaskReferenceList) == 0 {
		return nil
	}

	log := ctrl.LoggerFrom(ctx)
	var msgs []string
	for _, ref := range parent.SubtaskReferenceList {
		if ref == nil || ref.UUID == nil || *ref.UUID == "" {
			continue
		}
		childUUID := *ref.UUID
		if _, seen := visited[childUUID]; seen {
			continue
		}
		visited[childUUID] = struct{}{}

		child, err := client.V3.GetTask(ctx, childUUID)
		if err != nil {
			log.Error(err, "failed to get Prism subtask while collecting failure details", "subtaskUUID", childUUID)
			continue
		}
		if child.Status == nil || (*child.Status != "FAILED" && *child.Status != "INVALID_UUID") {
			continue
		}

		op := ptr.Deref(child.OperationType, "")
		if op == "" {
			op = "unknown"
		}
		detail := fmt.Sprintf("[%s] error_detail: %s, progress_message: %s",
			op, ptr.Deref(child.ErrorDetail, ""), ptr.Deref(child.ProgressMessage, ""))
		log.Info("failed Prism subtask",
			"subtaskUUID", childUUID,
			"operation", op,
			"errorDetail", ptr.Deref(child.ErrorDetail, ""),
			"progressMessage", ptr.Deref(child.ProgressMessage, ""),
		)
		msgs = append(msgs, detail)
		msgs = append(msgs, collectFailedV3SubtaskErrors(ctx, client, child, visited)...)
	}
	return msgs
}
