let list =
    [ "COLLEGE OF ARTS AND SCIENCES"
      "COLLEGE OF BUILT ENVIRONMENTS"
      "COLLEGE OF EDUCATION"
      "COLLEGE OF ENGINEERING"
      "COLLEGE OF THE ENVIRONMENT"
      "FOSTER SCHOOL OF BUSINESS"
      "INTERDISCIPLINARY UNDERGRADUATE PROGRAMS"
      "INTERSCHOOL OR INTERCOLLEGE PROGRAMS"
      "SCHOOL OF MEDICINE"
      "SCHOOL OF NURSING"
      "SCHOOL OF PUBLIC HEALTH"
      "SCHOOL OF SOCIAL WORK"
      "THE INFORMATION SCHOOL"
      "UW BOTHELL"
      "UW TACOMA" ]

let collegeMap =
    list
    |> List.mapi (fun i name -> i + int 'A' |> char, name)
    |> Map.ofList

let codeMap =
    list
    |> List.mapi (fun i name -> name, i + int 'A' |> char)
    |> Map.ofList
