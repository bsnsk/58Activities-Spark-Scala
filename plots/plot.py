#!/usr/bin/env python

# import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rc
import matplotlib.patches as mpatches
import matplotlib
import os
# import random
# import time

ext = ".eps"

matplotlib.rcParams.update({'font.size': 17})


def activityDayCount():
    labels = "2", "3", "4", "5", "6~9", "> 9"
    data = [474334, 192039, 97792, 56249, 81316, 25208]
    colors = [
            'lightcoral', 'lightblue', 'yellowgreen', 'gold', 'pink', 'orange'
            ]
    # labels = "2", "3", "4", "5", "6", "7", "8", "9", "> 9"
    # data = [474334, 192039, 97792, 56249, 34578, 22223, 14902, 9613, 25208]
    # colors = [
    #     'lightcoral', 'lightblue', 'yellowgreen', 'gold', 'green',
    #     'blue', 'purple', 'pink', 'orange', 'grey'
    # ]
    # cmap = plt.cm.prism
    # colors = cmap(np.linspace(0., 1., len(labels)))
    plt.axis('equal')
    plt.pie(
            data,
            labels=labels,
            autopct='%1.1f%%',
            shadow=True,
            colors=colors,
            startangle=90,
            pctdistance=0.81
            # labeldistance=0.9
    )
    filename = "./ActivityDay" + ext
    plt.savefig(filename)
    # os.system("open " + filename)


def powerActivityInterval():
    labels = "2", "3", "4", "5", "6~8", "> 8"
    data = [
        302574, 206186, 63559, 49204,
        21519 + 18555 + 9879,
        8945 + 5526 + 4941 + 3319 + 2873 + 2137 + 1825 + 912 + 1188 + 944
        + 1022 + 828 + 701 + 542 + 655 + 474 + 419 + 253 + 283 + 275 + 374
        + 358 + 623
        # 449895, 135273, 54127, 26426,
        # 14913 + 8692 + 5768,
        # 3788 + 2737 + 1947 + 1551 + 1249 + 937 + 766 + 694 + 515
        # + 431 + 272 + 227 + 194 + 135 + 88 + 110 + 52 + 41 + 38 + 15 + 10 + 1
        # + 1
            ]
    colors = [
            'lightcoral', 'lightblue', 'yellowgreen', 'gold', '#FF7BAC',
            '#8B9FC0', 'purple', 'pink'
            ]
    # colors = [
    #     'lightcoral', 'lightblue', 'yellowgreen', 'gold', 'pink',
    #     'orange'
    # ]
    plt.axis('equal')
    plt.pie(
            data,
            labels=labels,
            autopct='%1.1f%%',
            shadow=True,
            colors=colors,
            startangle=90,
            pctdistance=0.83
    )
    filename = "./ActivityInterval" + ext
    plt.savefig(filename)
    os.system("open " + filename)


def continusActivity():
    labels = "2", "3", "4", "5", "6~8", "> 8"
    data = [
         339315, 227716, 80042, 44103,
         21721 + 12851 + 7419, 4782 + 10453
            ]
    colors = [
            'lightcoral', 'lightblue', 'yellowgreen', 'gold', '#FF7BAC',
            '#8B9FC0', 'purple', 'pink'
            ]
    # colors = [
    #     'lightcoral', 'lightblue', 'yellowgreen', 'gold', 'pink',
    #     'orange'
    # ]
    plt.axis('equal')
    plt.pie(
            data,
            labels=labels,
            autopct='%1.1f%%',
            shadow=True,
            colors=colors,
            startangle=90,
            pctdistance=0.8
    )
    filename = "./ActivityDay" + ext
    plt.savefig(filename)
    # os.system("open " + filename)


def numberIterations():
    x = [
            1, 2, 3,
            5, 8, 10,
            12, 15
        ]
    y = [
            0.5, 0.63141093174, 0.697941532851,
            0.689625707331, 0.677482215081, 0.67721843094,
            0.677048542104, 0.679407512327
        ]
    plt.plot(x, y, marker='.')
    plt.axis([0, 16, 0.45, 0.75])
    plt.xlabel("# of iterations")
    plt.ylabel("AUC")
    filename = "./NumberIterations" + ext
    plt.savefig(filename)
    # os.system("open " + filename)


def parameterNumTrees():
    x = [3, 4, 6, 8, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    auc = [
        0.535178949203,
        0.542328048322,
        0.53965360195,
        0.540522688697,
        0.539786324703,
        0.538543954391,
        0.538709940309,
        0.538486978138,
        0.538511488698,
        0.53841030334,
        0.537832963885,
        0.537795069212,
        0.538533024443
        ]
    plt.xlabel("\# of trees in Random Forest")
    plt.ylabel("Measurement of prediction result")
    plt.plot(x, auc, 'r')
    rc('text', usetex=True)
    filename = "./ParameterNumTrees" + ext
    plt.savefig(filename)
    # os.system("open " + filename)


def parameterMaxDepth():
    x = [1, 3, 6, 9, 10, 12, 15, 18]
    auc = [
        0.5,
        0.530167110714,
        0.534175309461,
        0.537991447336,
        0.538511488698,
        0.538747477529,
        0.53919721591,
        0.538927390763
    ]
    plt.xlabel("Max tree depth in Random Forest")
    plt.ylabel("Measurement of prediction result")
    plt.plot(x, auc, 'r')
    rc('text', usetex=True)
    filename = "./ParameterMaxDepth" + ext
    plt.savefig(filename)
    os.system("open " + filename)


def parameterGamma():
    x = [
            0, 0.025, 0.05, 0.075,
            0.1, 0.125, 0.15, 0.175,
            0.2, 0.225, 0.25, 0.275,
            0.3
        ]
    auc = [
            0.801180577625, 0.803572573946, 0.80489984546, 0.80377223374,
            0.813605079096, 0.813130073065, 0.813966457008, 0.816743640758,
            0.816091040286, 0.8112706937, 0.813802511038, 0.804321627593,
            0.798891057248
        ]
    precision = [
            0.653261933302, 0.602246598249, 0.630421127424, 0.6117061573,
            0.471436915914, 0.467027128287, 0.4620003597, 0.454860270582,
            0.438033756609, 0.373874506211, 0.370803143271, 0.310969203119,
            0.281924350052
        ]
    recall = [
            0.64107751479, 0.656484733573, 0.653340275095, 0.654794689601,
            0.724548740985, 0.719634026102, 0.723789756312, 0.733451003213,
            0.741407202533, 0.769272115752, 0.777675255774, 0.813956968845,
            0.841884015142
        ]
    plt.plot(x, auc, 'r-', label="AUC", marker='s')
    plt.plot(x, precision, 'b--', label="Precision", marker='o')
    plt.plot(x, recall, 'g-.', label="Recall", marker='x')
    rc('text', usetex=True)
    plt.xlabel("Parameter $\gamma$")
    plt.ylabel("Measurement of prediction result")
    plt.legend(
        bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
        ncol=3, mode="expand", borderaxespad=0.
    )
    # handles = [
    #         mpatches.Patch(color='red', label='AUC'),
    #         mpatches.Patch(color='blue', label='Precision'),
    #         mpatches.Patch(color='green', label='Recall')
    #         ]
    # plt.legend(
    #         handles=handles, bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
    #         ncol=3, mode="expand", borderaxespad=0.
    #     )
    filename = "./ParameterGamma" + ext
    plt.savefig(filename)
    # os.system("open " + filename)


def fixedSampleActivities():
    x = range(0, 32)
    u1 = [
            0, 0, 3, 9, 3,
            0, 0, 0, 0, 0,
            1, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 0
        ]
    u2 = [
            0, 0, 0, 0, 8,
            0, 0, 3, 1, 5,
            0, 0, 1, 0, 0,
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 0
        ]
    u3 = [
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            0, 0, 6, 3, 0,
            0, 0, 0, 3, 0,
            0, 0, 0, 0, 0,
            0, 0, 0, 4, 3,
            2, 0
        ]
    plt.plot(x, u1, 'r-', label="User a", marker='s')
    plt.plot(x, u2, 'g--', label="User b", marker='o')
    plt.plot(x, u3, 'b-.', label="User c", marker='x')
    plt.axis([0, 35, 0, 10])
    rc('text', usetex=True)
    plt.xlabel("Day")
    plt.ylabel("Number of Activities")
    plt.legend(
        bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
        ncol=3, mode="expand", borderaxespad=0.
    )
    filename = "./FixedSampleActivities" + ext
    plt.savefig(filename)
    # os.system("open " + filename)

# fixedSampleActivities()
# parameterMaxDepth()
# parameterNumTrees()
# powerActivityInterval()
parameterGamma()
# numberIterations()
# continusActivity()
# activityDayCount()
# plt.show()
