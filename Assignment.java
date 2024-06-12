package Assignment;

import Entites.Queue;
import Entites.SystemGraph;
import Entites.TaskGraph;
import Support.SupportV4PrintMatrix;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Assignment
{
    TaskGraph taskGraph;
    SystemGraph systemGraph;
    public Integer[][] processingAssignmentByTaskId;
    // task id | processorId, time
    public Integer[][] processingAssignmentByProcessor;
    // processorId, time | task id
    List<Integer[]> tranferAssignmentList;
    // <processorId, processorIdTarget, taskIdFrom, taskIdTo, time, lengthInTime>
    boolean[][] tranferAssignmentListCompleted;
    // task id from | task id to
    List<Integer[]> readAssignmentList;
    // <processorId, processorIdTarget, taskIdFrom, taskIdTo, time, lengthInTime>
    boolean[][] readAssignmentListCompleted;
    // task id from | task id to
    public boolean processorsBuisy[][];
    // processor id, time | false/true free/buisy
    public boolean[] processorsAssigned;
    public int lastestTime;

    public enum AsiignmemtType
    {
        RANDOM,
        NEIGHBOUR
    }

    public Assignment(AsiignmemtType assignmemtType, TaskGraph taskGraph, Queue queue, SystemGraph systemGraph)
    {
        int massiveSize = 10000;
        this.taskGraph = taskGraph;
        this.systemGraph = systemGraph;
        processingAssignmentByTaskId = new Integer[taskGraph.size][2];
        processingAssignmentByProcessor = new Integer[systemGraph.size][massiveSize];
        processorsBuisy = new boolean[systemGraph.size][massiveSize];
        processorsAssigned = new boolean[systemGraph.size];
        lastestTime = 0;
        tranferAssignmentList = new ArrayList<>();
        tranferAssignmentListCompleted = new boolean[taskGraph.size][taskGraph.size];
        readAssignmentList = new ArrayList<>();
        readAssignmentListCompleted = new boolean[taskGraph.size][taskGraph.size];
        generateSmP(assignmemtType, taskGraph, queue, systemGraph);
    }


    private void generateSmP(AsiignmemtType asiignmemtType, TaskGraph taskGraph, Queue queue, SystemGraph systemGraph)
    {
        Random random = new Random();
        //int time = 0;
        // перебираємо значення черги!! может, лучше пербирать время? перебор времени более стабильній
        // Модифікація
        // Черга збиває місця попередників, і виходять завдання, попердники яких ще не виконалися.
        // Тому у головному циклі вибираємо з черги перше завдання, яке виконалося
        //for (int idInQueue = 0; idInQueue < queue.queue.length; idInQueue++)
        for (int taskNumber = 0; taskNumber < queue.queue.length; taskNumber++)
        {
            //SupportV4PrintMatrix.printMatrix(taskGraph.connectionsWeight);

            Integer thisTaskId = null;
            Integer idInQueue = null;
            //boolean settledForThisTask = false;

            for (int checkedIdInQueue = 0; checkedIdInQueue < queue.queue.length; checkedIdInQueue++)
            {
                int checkedTaskId = queue.queue[checkedIdInQueue];
                if (this.processingAssignmentByTaskId[checkedTaskId][0] == null)
                {
                    boolean predcessorTaskNotCompleted = false;
                    for (int possiblePredcessorTaskId = 0; possiblePredcessorTaskId < taskGraph.size; possiblePredcessorTaskId++)
                    {
                        if (taskGraph.connectionsWeight[possiblePredcessorTaskId][checkedTaskId] != null)
                        {
                            if (this.processingAssignmentByTaskId[possiblePredcessorTaskId][0] == null) {predcessorTaskNotCompleted = true; break;}
                        }
                    }
                    if (predcessorTaskNotCompleted == false)
                    {
                        //settledForThisTask = true;
                        thisTaskId = checkedTaskId;
                        idInQueue = checkedIdInQueue;
                        break;
                    }
                }
            }
            //int thisTaskId = queue.queue[idInQueue];

            // Визначаємо завдання-попередники і записуємо їх номер до predcessorTasksIds
            List<Integer> predcessorTasksIds = new ArrayList<>();
            for (int taskId = 0; taskId < taskGraph.size; taskId++)
            {
                if((taskGraph.connectionsWeight[taskId][thisTaskId] != null))
                {
                    predcessorTasksIds.add(taskId);
                }
            }

            int selectedProcessorId = 0;
            int assignmentTime;

            //boolean predcessorsPresent= false;
            if (predcessorTasksIds.size() > 0)
            {
                // AT LEAST ONE PREDCESSOR

                // Визначаємо найпізніший час виконання завдання-попередника і записуємо до lastestTime
                int lastestTime = 0;
                for (int id : predcessorTasksIds)
                {
                    int possibleLastestTime = processingAssignmentByTaskId[id][1] + taskGraph.nodesWeight[id];
                    if(possibleLastestTime > lastestTime)
                    {
                        lastestTime = possibleLastestTime;
                    }
                }

                //============================================================================================

                if(asiignmemtType.equals(AsiignmemtType.RANDOM))
                {
                    int minimumEarliestFreeTimeBeginning = Integer.MAX_VALUE;
                    List<Integer> processorsIdsWithMinimumEarliestFreeTimeBeginning = new ArrayList<>();
                    // Визначаємо процесори, що звільниться на необхідну кількість часу якнайраніше.
                    for (int processorId = 0; processorId < systemGraph.size; processorId++)
                    {
                        int earliestFreeTimeBeginning
                                = findProcessorFreeTimeForOperation(processorId, lastestTime, thisTaskId);
                        if(earliestFreeTimeBeginning < minimumEarliestFreeTimeBeginning)
                        {
                            minimumEarliestFreeTimeBeginning = earliestFreeTimeBeginning;
                            processorsIdsWithMinimumEarliestFreeTimeBeginning.clear();
                            processorsIdsWithMinimumEarliestFreeTimeBeginning.add(processorId);
                        }
                        else if(earliestFreeTimeBeginning == minimumEarliestFreeTimeBeginning)
                        {
                            processorsIdsWithMinimumEarliestFreeTimeBeginning.add(processorId);
                        }
                    }
                    // Якщо їх кілька - випадковим чином визначаємо один з них
                    if(processorsIdsWithMinimumEarliestFreeTimeBeginning.size() == 0)
                    {
                        selectedProcessorId = processorsIdsWithMinimumEarliestFreeTimeBeginning.get(0);
                    }
                    else if (processorsIdsWithMinimumEarliestFreeTimeBeginning.size() > 0)
                    {
                        int randomId = random.nextInt(processorsIdsWithMinimumEarliestFreeTimeBeginning.size());
                        selectedProcessorId = processorsIdsWithMinimumEarliestFreeTimeBeginning.get(randomId);
                    }
                }
                else if(asiignmemtType.equals(AsiignmemtType.NEIGHBOUR))
                {

                    // Визначаємо процесор, який зберігає якнайбільше необхідних даних -------------------------
                    //// Разом з цим визначаємо процесори, на яких викнувалися завдання-попередники
                    ////List<Integer> IdsOfprocessorsWithPredcessorsTaks = new ArrayList<>();
                    // Рахуємо кількість даних в процесорах
                    int[] dataInProcessors = new int[systemGraph.size];
                    for (int id : predcessorTasksIds)
                    {
                        dataInProcessors[processingAssignmentByTaskId[id][0]]
                                = dataInProcessors[processingAssignmentByTaskId[id][0]]
                                + taskGraph.connectionsWeight[id][thisTaskId];
                    }
                    // Визначаємо процесори з найбільшою кількістю даних
                    int maxData = 0;
                    List<Integer> processorIdsWithMaxData = new ArrayList<>();
                    for (int processorId = 0; processorId < systemGraph.size; processorId++)
                    {
                        if(dataInProcessors[processorId] == maxData)
                        {
                            processorIdsWithMaxData.add(processorId);
                        }
                        else if(dataInProcessors[processorId] > maxData)
                        {
                            processorIdsWithMaxData.clear();
                            processorIdsWithMaxData.add(processorId);
                            maxData = dataInProcessors[processorId];
                        }
                    }
                    //// Визначаємо процесори, на яких виконувалися завдання-попередники
                    ////for (int processorId = 0; processorId < systemGraph.size; processorId++)
                    ////{
                    ////    if(dataInProcessors[processorId] > 0)
                    ////    {
                    ////        IdsOfprocessorsWithPredcessorsTaks.add(processorId);
                    ////    }
                    ////}


                    // Серед процесорів з найбільшою кількістю даних
                    // Визначаємо для кожного найраніший час, коли він звільниться
                    int[] processorsEarlistFreeTimeInProcessorIdsWithMaxData = new int[processorIdsWithMaxData.size()];
                    for (int i= 0; i < processorIdsWithMaxData.size(); i++)
                    {
                        int processorId = processorIdsWithMaxData.get(i);
                        // добавляем дополнительную переменную вместо записи сразу в массив только для наглядности
                        int earliestFreeTimeBeginning = Integer.MAX_VALUE;
                        int currentTaskTimeIteration = 0;
                        for (int timeI = lastestTime; ; timeI++)
                        {
                            if(processorsBuisy[processorId][timeI] == false)
                            {
                                if(currentTaskTimeIteration == 0)
                                {
                                    earliestFreeTimeBeginning = timeI;
                                }
                                // розриваємо цикл, якщо знайдено вільного часу достатньо для виконанння процесу
                                if (currentTaskTimeIteration == taskGraph.nodesWeight[thisTaskId])
                                {
                                    break;
                                }
                                currentTaskTimeIteration++;
                            }
                            else if(processorsBuisy[processorId][timeI] == true)
                            {
                                // обнуляем только для наглядности
                                earliestFreeTimeBeginning = Integer.MAX_VALUE;
                                currentTaskTimeIteration = 0;
                            }
                        }
                        processorsEarlistFreeTimeInProcessorIdsWithMaxData[i] = earliestFreeTimeBeginning;
                    }

                    // Визначаємо процесори з найпізнішим відповідним вільним часом
                    int latestFreeTime = 0;
                    List<Integer> latestFreeTimeProcessorIds = new ArrayList<>();
                    for (int i = 0; i < processorIdsWithMaxData.size(); i++)
                    {
                        int processorId = processorIdsWithMaxData.get(i);
                        if(dataInProcessors[processorId] == maxData)
                        {
                            latestFreeTimeProcessorIds.add(processorId);
                        }
                        if(processorsEarlistFreeTimeInProcessorIdsWithMaxData[i] > latestFreeTime)
                        {
                            latestFreeTimeProcessorIds.clear();
                            latestFreeTimeProcessorIds.add(processorId);
                            latestFreeTime = processorsEarlistFreeTimeInProcessorIdsWithMaxData[i];
                        }
                    }
                    // обираємо єдиний або перший
                    selectedProcessorId = latestFreeTimeProcessorIds.get(0);
                }




                // Робимо пересилання в спільну пам'ять і зчитування зі спільної пам'яті ----------------------
                // Перебираємо завдання - попередники їх процесори і визначаємо час після виконання завдання-попередника,
                // коли вони матимуть достатньо часу для пересилки даних з завдання-попередника
                // В цей час записуємо пересилку-запис
                int lastestOperationEnd = 0;
                int firstAvaliableAssignmentTime = Integer.MAX_VALUE;
                for (int i = 0; i < predcessorTasksIds.size(); i++)
                /////////for (int processorId: IdsOfprocessorsWithPredcessorsTaks)
                {
                    int predcessorTaskId = predcessorTasksIds.get(i);
                    // Для задач, выполененных на этом процессоре пересылка не надо - их результат записан в кэше процессора
                    if(processingAssignmentByTaskId[predcessorTaskId][0] != selectedProcessorId)
                    {
                        int freeTimeFirstTransfer = findProcessorFreeTimeForOperation
                                (processingAssignmentByTaskId[predcessorTaskId][0],
                                        processingAssignmentByTaskId[predcessorTaskId][1] + taskGraph.nodesWeight[predcessorTaskId],
                                        taskGraph.connectionsWeight[predcessorTaskId][thisTaskId]);

                        // Призначаємо на процесор пересилку
                        tranferAssignmentList.add
                        (
                            // <processorId, processorIdTarget, taskIdFrom, taskIdTo, time, lengthInTime>
                            new Integer[]
                            {
                                processingAssignmentByTaskId[predcessorTaskId][0], selectedProcessorId,
                                predcessorTaskId, thisTaskId, freeTimeFirstTransfer,
                                taskGraph.connectionsWeight[predcessorTaskId][thisTaskId]
                            }
                        );
                        // task id from | task id to
                        tranferAssignmentListCompleted[thisTaskId][predcessorTaskId] = true;
                        for (int time2 = 0; time2 < taskGraph.connectionsWeight[predcessorTaskId][thisTaskId]; time2++)
                        {
                            processorsBuisy[processingAssignmentByTaskId[predcessorTaskId][0]][freeTimeFirstTransfer+time2] = true;
                            if (lastestOperationEnd < (freeTimeFirstTransfer + time2)) { lastestOperationEnd = freeTimeFirstTransfer + time2; }
                        }

                        ////////////////////////////////////////////////////////////////////////////////////////////////

                        int freeTimeFirstRead = findProcessorFreeTimeForOperation
                                (selectedProcessorId,
                                        freeTimeFirstTransfer + taskGraph.connectionsWeight[predcessorTaskId][thisTaskId],
                                        taskGraph.connectionsWeight[predcessorTaskId][thisTaskId]);
                        readAssignmentList.add
                        (
                            // <processorId, processorIdTarget, taskIdFrom, taskIdTo, time, lengthInTime>
                            new Integer[]
                            {
                                processingAssignmentByTaskId[predcessorTaskId][0], selectedProcessorId,
                                predcessorTaskId, thisTaskId, freeTimeFirstRead,
                                taskGraph.connectionsWeight[predcessorTaskId][thisTaskId]
                            }
                        );
                        // task id from | task id to
                        readAssignmentListCompleted[thisTaskId][predcessorTaskId] = true;
                        for (int time2 = 0; time2 < taskGraph.connectionsWeight[predcessorTaskId][thisTaskId]; time2++)
                        {
                            processorsBuisy[selectedProcessorId][freeTimeFirstRead+time2] = true;
                            if (lastestOperationEnd < (freeTimeFirstRead + time2)) { lastestOperationEnd = freeTimeFirstTransfer + time2; }
                        }

                        firstAvaliableAssignmentTime = lastestOperationEnd + 1;
                    }
                    else if(processingAssignmentByTaskId[predcessorTaskId][0] == selectedProcessorId)
                    {
                        int posiibleLastestOperationEnd = processingAssignmentByTaskId[predcessorTaskId][1];
                        if (lastestOperationEnd < (posiibleLastestOperationEnd)) { lastestOperationEnd = posiibleLastestOperationEnd; }
                        firstAvaliableAssignmentTime = lastestOperationEnd;
                    }
                }
                assignmentTime = findProcessorFreeTimeForOperation(selectedProcessorId, firstAvaliableAssignmentTime,
                        taskGraph.nodesWeight[thisTaskId]);
            }
            else
            {
                // NO PREDCESSORS
                // Перебираємо процесори і шукамо процесори, на які ще нічого не призначалося
                // Якщо такий знаходиться - обираємо його для призначення.
                // Якщо таких нема - перебираємо процесори
                // і шукаємо процесор з найближчим відповідним до завдання вільним часом.

                boolean foundFreeProcessor = false;
                for (int processorId = 0; processorId < processorsAssigned.length; processorId++)
                {
                    if (processorsAssigned[processorId] == false)
                    {
                        selectedProcessorId = processorId;
                        foundFreeProcessor= true;
                        break;
                    }
                }
                if (foundFreeProcessor == true)
                {
                    assignmentTime = 0;
                }

                else
                {
                    int earliestFreeTime = Integer.MAX_VALUE;
                    int earliestFreeTimeProcessorId = 0;
                    for (int processorId = 0; processorId < systemGraph.size; processorId++)
                    {
                        int freeTimeFirst = findProcessorFreeTimeForOperation
                                (processorId,0, taskGraph.nodesWeight[thisTaskId]);
                        if (freeTimeFirst < earliestFreeTime)
                        {
                            earliestFreeTimeProcessorId = processorId;
                            earliestFreeTime = freeTimeFirst;
                        }
                    }
                    selectedProcessorId = earliestFreeTimeProcessorId;
                    assignmentTime = earliestFreeTime;
                }
            }

            // Призначаємо на обраний процесор завдання
            processorsAssigned[selectedProcessorId] = true;
            processingAssignmentByProcessor[selectedProcessorId][assignmentTime] = queue.queue[idInQueue];
            processingAssignmentByTaskId[thisTaskId][0] = selectedProcessorId;
            processingAssignmentByTaskId[thisTaskId][1] = assignmentTime;
            // заповнюємо таблицю зайнятих процесорів за часом
            for (int time2 = 0; time2 < taskGraph.nodesWeight[queue.queue[idInQueue]]; time2++)
            {
                processorsBuisy[selectedProcessorId][assignmentTime+time2] = true;
                updateLastestTimeIfNecessary(assignmentTime+time2);
            }
        }
    }


    private void updateLastestTimeIfNecessary(int time)
    {
        if (lastestTime < time) { lastestTime = time; }
    };


    private int findProcessorFreeTimeForOperation(int processorId, int beginningTime, int requiredTime)
    {
        int earliestFreeTimeBeginning = Integer.MAX_VALUE;
        int currentTaskTimeIteration = 0;
        for (int timeI = beginningTime; ; timeI++)
        {
            if(processorsBuisy[processorId][timeI] == false)
            {
                if(currentTaskTimeIteration == 0)
                {
                    earliestFreeTimeBeginning = timeI;
                }
                // розриваємо цикл, якщо знайдено вільного часу достатньо для виконанння процесу
                if (currentTaskTimeIteration == requiredTime)
                {
                    break;
                }
                currentTaskTimeIteration++;
            }
            else if(processorsBuisy[processorId][timeI] == true)
            {
                // обнуляем только для наглядности
                earliestFreeTimeBeginning = Integer.MAX_VALUE;
                currentTaskTimeIteration = 0;
            }
        }
        return(earliestFreeTimeBeginning);
    }


    public void printGauntDiagram()
    {
        System.out.println("Діаграма Ганта");
        SupportV4PrintMatrix.printMatrix(SupportPrintGauntDiagram.printGauntDiagram(this.taskGraph, this.systemGraph, this));
        System.out.print("\n");
    }
}
