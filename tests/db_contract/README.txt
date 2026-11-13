DB 팀 DB Lambda v2 전달본(carecompass_db_lambda_v2_cognito.zip, 2026-11-06 게시) src/ 의 model_validation.py, common.py 를 수정 없이 복사.
(이전 전달본 1.1 대비 model_validation.py는 동일, common.py는 인증·감사 부분만 변경)
파서·적재 출력이 쓰기 Lambda 검증을 통과하는지 로컬에서 확인하는 용도. 원본이 바뀌면 다시 복사한다.

(공개 저장소용: common.py의 기본 버킷 이름에서 AWS 계정 ID만 <ACCOUNT_ID>로 가렸습니다. 테스트에는 영향 없음)
