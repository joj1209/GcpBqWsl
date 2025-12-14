#!/bin/bash

# HyunCard Shell Script
# 작성일: 2025-12-13

echo "======================================"
echo "HyunCard 쉘 스크립트 실행 시작"
echo "======================================"
echo ""

# 현재 시간 출력
echo "현재 시간: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 시스템 정보 출력
echo "시스템 정보:"
echo "- 사용자: $USER"
echo "- 호스트: $HOSTNAME"
echo "- 작업 디렉토리: $(pwd)"
echo "- Shell: $SHELL"
echo ""

# 간단한 작업 예제
echo "파일 목록 확인:"
ls -lh
echo ""

echo "======================================"
echo "스크립트 실행 완료"
echo "======================================"
